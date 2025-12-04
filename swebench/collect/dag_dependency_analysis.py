"""
DAG-based dependency analysis for PR chains.

This module implements sophisticated dependency detection between PRs using:
- File overlap detection (including handling of renamed files)
- Temporal proximity filtering
- Issue relationship matching

The result is a Directed Acyclic Graph (DAG) of dependencies from which
diverse chains can be sampled.

NOTE: Task instances in the DAG should ideally have FAIL_TO_PASS tests populated.
If instances don't have FAIL_TO_PASS tests, chain validation may report warnings.
This is expected behavior when working with newly created task instances that
haven't been processed through the full test identification pipeline.
"""

import logging
import random
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Optional, Set

import docker
import unidiff
from tqdm import tqdm

from swebench.harness.constants import (
    DOCKER_PATCH,
    DOCKER_USER,
    DOCKER_WORKDIR,
    UTF8,
)
from swebench.harness.docker_build import build_container, setup_logger
from swebench.harness.docker_utils import (
    cleanup_container,
    copy_to_container,
    exec_run_with_timeout,
)
from swebench.harness.grading import get_logs_eval
from swebench.harness.test_spec.test_spec import make_test_spec

logger = logging.getLogger(__name__)


# Type alias for PR sampler function
# Takes: (leaf_prs: list[int], dag: DependencyDAG, covered_files: Set[str], seed: Optional[int]) -> int
PRSampler = Callable[[list[int], "DependencyDAG", Set[str], Optional[int]], int]


@dataclass
class PRNode:
    """Represents a PR in the dependency DAG.

    All fields from the original task instance are preserved in the task_instance
    dictionary, including version information if present (e.g., from get_versions.py).
    """

    pr_number: int
    instance_id: str
    created_at: datetime
    base_commit: str
    issues: Set[str]
    modified_files_pre: Set[
        str
    ]  # files before PR (deleted files, old names of renames)
    modified_files_post: Set[str]  # files after PR (new files, new names of renames)
    version: Optional[str]  # version info from get_versions.py, if available
    task_instance: Dict[str, Any]  # full task instance with all original fields


@dataclass
class DependencyDAG:
    """Directed Acyclic Graph of PR dependencies."""

    nodes: Dict[int, PRNode] = field(default_factory=dict)
    edges: Dict[int, Dict[int, float]] = field(
        default_factory=dict
    )  # pr -> {dependency -> weight}

    def add_node(self, node: PRNode) -> None:
        """Add a PR node to the DAG."""
        self.nodes[node.pr_number] = node
        if node.pr_number not in self.edges:
            self.edges[node.pr_number] = {}

    def add_edge(self, from_pr: int, to_pr: int, weight: float) -> None:
        """Add a dependency edge with weight."""
        if from_pr not in self.edges:
            self.edges[from_pr] = {}
        self.edges[from_pr][to_pr] = weight

    def get_dependencies(self, pr_number: int) -> list[int]:
        """Get PRs that this PR depends on."""
        return list(self.edges.get(pr_number, {}).keys())

    def get_dependents(self, pr_number: int) -> list[int]:
        """Get PRs that depend on this PR."""
        return [
            pr for pr, dependencies in self.edges.items() if pr_number in dependencies
        ]

    def get_dependency_weights(self, pr_number: int) -> Dict[int, float]:
        """Get PRs that this PR depends on with their weights."""
        assert pr_number in self.edges, f"PR {pr_number} not found in DAG edges"
        return self.edges[pr_number]

    def get_dependent_weights(self, pr_number: int) -> Dict[int, float]:
        """Get PRs that depend on this PR with their weights."""
        return {
            pr: weight
            for pr, dependencies in self.edges.items()
            for dependency, weight in dependencies.items()
            if dependency == pr_number
        }

    def get_topological_order(self) -> list[int]:
        """Return PRs in topological order (dependencies before dependents)."""
        in_degree = {pr: 0 for pr in self.nodes}
        for deps in self.edges.values():
            for dep in deps.keys():
                in_degree[dep] += 1

        queue = [pr for pr, deg in in_degree.items() if deg == 0]
        result = []

        while queue:
            pr = queue.pop(0)
            result.append(pr)
            for dep in self.edges.get(pr, {}).keys():
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)

        return result


def extract_modified_files_pre(patch_str: str) -> Set[str]:
    """
    Extract files that existed BEFORE the PR (pre-state).

    This includes:
    - Files that were modified (existed before, still exist after)
    - Files that were deleted (existed before, don't exist after)
    - Old names of renamed files (existed before as old name)

    Excludes:
    - Newly created files (didn't exist before)
    """
    if not patch_str or not patch_str.strip():
        return set()

    files_pre = set()

    try:
        patch_set = unidiff.PatchSet(patch_str)
        for patched_file in patch_set:
            if patched_file.is_added_file:
                # Skip newly created files - they didn't exist before
                continue
            elif patched_file.is_removed_file:
                # Deleted file - existed before
                file_path = (
                    patched_file.source_file[2:]
                    if patched_file.source_file.startswith("a/")
                    else patched_file.source_file
                )
                files_pre.add(file_path)
            elif patched_file.is_rename:
                # Renamed file - old name existed before
                old_path = (
                    patched_file.source_file[2:]
                    if patched_file.source_file.startswith("a/")
                    else patched_file.source_file
                )
                files_pre.add(old_path)
            else:
                # Modified file - existed before
                files_pre.add(patched_file.path)
        return files_pre
    except Exception as e:
        logger.warning(f"Failed to parse patch for pre-files with unidiff: {e}")
        return set()


def extract_modified_files_post(patch_str: str) -> Set[str]:
    """
    Extract files that exist AFTER the PR (post-state).

    This includes:
    - Files that were modified (existed before, still exist after)
    - Files that were created (didn't exist before, exist after)
    - New names of renamed files (exist after as new name)

    Excludes:
    - Files that were deleted (existed before, don't exist after)
    """
    if not patch_str or not patch_str.strip():
        return set()

    files_post = set()

    try:
        patch_set = unidiff.PatchSet(patch_str)
        for patched_file in patch_set:
            if patched_file.is_removed_file:
                # Skip deleted files - they don't exist after
                continue
            elif patched_file.is_added_file:
                # Created file - exists after
                files_post.add(patched_file.path)
            elif patched_file.is_rename:
                # Renamed file - new name exists after
                new_path = (
                    patched_file.target_file[2:]
                    if patched_file.target_file.startswith("b/")
                    else patched_file.target_file
                )
                files_post.add(new_path)
            else:
                # Modified file - exists after
                files_post.add(patched_file.path)
        return files_post
    except Exception as e:
        logger.warning(f"Failed to parse patch for post-files with unidiff: {e}")
        return set()


def calculate_file_overlap_weight(target_pr: PRNode, candidate_pr: PRNode) -> float:
    """
    Calculate file overlap weight between two PRs based on pre/post file states.

    Dependencies are detected when:
    1. Target PR modifies files that candidate PR also modified (pre->pre or post->post)
    2. Target PR modifies files that candidate PR created (post->post)
    3. Target PR modifies files that candidate PR deleted (pre->pre)
    4. Target PR touches the post-state of files that candidate PR touched

    Args:
        target_pr: The PR to analyze dependencies for (happens after candidate)
        candidate_pr: A potential dependency PR (happens before target)

    Returns:
        Weight between 0.0 and 1.0 representing the strength of the file overlap
    """
    # Get all pre-existing files that target PR touched
    target_files = target_pr.modified_files_pre

    if not target_files:
        return 0.0

    overlapping_files = target_files & candidate_pr.modified_files_post

    if not overlapping_files:
        return 0.0

    # Calculate weight as ratio of overlapping files to total modified pre-existing files in target PR
    weight = len(overlapping_files) / len(target_files)

    return weight


def build_dependency_dag(
    task_instances: list[Dict[str, Any]],
    time_window_months: int = 6,
    file_overlap_threshold: float = 0.2,
) -> DependencyDAG:
    """
    Build a dependency DAG from task instances using file overlap analysis.

    Algorithm:
    1. Sort PRs by date (newest to oldest)
    2. For each PR, examine all earlier PRs:
       a. Same issue → automatic dependency (weight 1.0)
       b. >6 months old → skip
       c. No file overlap → skip
       d. Otherwise → calculate file overlap weight
       e. If overlap weight > threshold → add dependency

    Args:
        task_instances: List of task instance dictionaries
        time_window_months: Maximum age difference for dependencies (default: 6)
        file_overlap_threshold: Minimum file overlap weight for dependency (default: 0.2)

    Returns:
        DependencyDAG with nodes and weighted edges
    """
    dag = DependencyDAG()

    # Parse task instances into PR nodes
    pr_nodes = []
    for instance in task_instances:
        # Parse creation date
        created_at_str = instance.get("created_at", "")
        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except:
            created_at = datetime.now()

        # Parse patch to get pre/post file states
        patch = instance.get("patch", "")
        modified_files_pre = extract_modified_files_pre(patch)
        modified_files_post = extract_modified_files_post(patch)

        # Extract issues
        issues = set(str(issue) for issue in instance.get("issue_numbers", []))

        node = PRNode(
            pr_number=instance["pull_number"],
            instance_id=instance["instance_id"],
            created_at=created_at,
            base_commit=instance.get("base_commit", ""),
            issues=issues,
            modified_files_pre=modified_files_pre,
            modified_files_post=modified_files_post,
            version=instance.get("version"),  # version info from get_versions.py
            task_instance=instance,  # full instance preserved with all fields
        )
        pr_nodes.append(node)
        dag.add_node(node)

    # Sort by date (newest first)
    pr_nodes.sort(key=lambda x: x.created_at, reverse=True)

    # Process each PR against all earlier PRs
    stats = {
        "total_comparisons": 0,
        "filtered_time": 0,
        "filtered_no_file_overlap": 0,
        "issue_based": 0,
        "file_overlap_checked": 0,
        "file_overlap_below_threshold": 0,
        "file_overlap_based": 0,
    }

    for i, target_pr in enumerate(
        tqdm(pr_nodes, desc="Analyzing PR dependencies", leave=False)
    ):
        logger.debug(f"Analyzing dependencies for PR {target_pr.pr_number}")

        # Look at all earlier PRs (later in list due to reverse sort)
        candidates_in_window = 0
        for candidate_pr in pr_nodes[i + 1 :]:
            stats["total_comparisons"] += 1

            # Filter 1: Check temporal proximity
            age_diff = target_pr.created_at - candidate_pr.created_at
            if age_diff.days > time_window_months * 30:
                stats["filtered_time"] += 1
                continue

            candidates_in_window += 1

            # Filter 2: Check for shared issues (automatic dependency)
            if target_pr.issues and candidate_pr.issues:
                if target_pr.issues & candidate_pr.issues:
                    dag.add_edge(target_pr.pr_number, candidate_pr.pr_number, 1.0)
                    logger.info(
                        f"  → Issue-based dependency on PR {candidate_pr.pr_number}"
                    )
                    stats["issue_based"] += 1
                    continue

            # Filter 3: Calculate file overlap weight
            stats["file_overlap_checked"] += 1
            overlap_weight = calculate_file_overlap_weight(target_pr, candidate_pr)

            if overlap_weight > file_overlap_threshold:
                dag.add_edge(
                    target_pr.pr_number, candidate_pr.pr_number, overlap_weight
                )
                logger.debug(
                    f"  → File overlap dependency on PR {candidate_pr.pr_number} (weight: {overlap_weight:.2f})"
                )
                stats["file_overlap_based"] += 1
            elif overlap_weight > 0:
                stats["file_overlap_below_threshold"] += 1
                logger.debug(
                    f"  File overlap with PR {candidate_pr.pr_number} below threshold: {overlap_weight:.2f}"
                )
            else:
                stats["filtered_no_file_overlap"] += 1

        if candidates_in_window > 0:
            logger.debug(
                f"  Checked {candidates_in_window} candidates within time window"
            )

    # Log final statistics
    logger.info("\n" + "=" * 60)
    logger.info("DAG Construction Statistics:")
    logger.info(f"  Total PR comparisons: {stats['total_comparisons']:,}")
    logger.info(f"  Filtered by time window: {stats['filtered_time']:,}")
    logger.info(f"  Filtered by no file overlap: {stats['filtered_no_file_overlap']:,}")
    logger.info(f"  Issue-based dependencies found: {stats['issue_based']}")
    logger.info(f"  File overlap checks performed: {stats['file_overlap_checked']:,}")
    logger.info(
        f"  File overlap-based dependencies found: {stats['file_overlap_based']}"
    )
    logger.info(
        f"  File overlap below threshold: {stats['file_overlap_below_threshold']:,}"
    )
    logger.info("=" * 60 + "\n")

    return dag


def file_coverage_sampler(
    leaf_prs: list[int],
    dag: DependencyDAG,
    covered_files: Set[str],
    seed: Optional[int] = None,
) -> int:
    """
    Sample PR that maximizes file coverage diversity.

    Picks the PR that touches the most files not yet covered by selected chains.
    If there are ties, randomly selects from PRs with maximum uncovered files.

    Args:
        leaf_prs: List of PR numbers to sample from
        dag: Dependency DAG containing PR nodes
        covered_files: Set of file paths already covered by previous chains
        seed: Random seed for deterministic tie-breaking

    Returns:
        Selected PR number that maximizes uncovered files
    """
    # Calculate uncovered file counts for all PRs (using all files touched by PR)
    uncovered_counts = [
        len(
            (dag.nodes[pr].modified_files_pre | dag.nodes[pr].modified_files_post)
            - covered_files
        )
        for pr in leaf_prs
    ]

    # Find maximum uncovered file count
    max_uncovered = max(uncovered_counts)

    # Get all PRs with maximum uncovered files
    best_prs = [
        pr for pr, count in zip(leaf_prs, uncovered_counts) if count == max_uncovered
    ]

    # Randomly select from best PRs (with optional seed for determinism)
    if seed is not None:
        random.seed(seed)

    return random.choice(best_prs)


def random_sampler(
    leaf_prs: list[int],
    dag: DependencyDAG,
    covered_files: Set[str],
    seed: Optional[int] = None,
) -> int:
    """
    Random PR selection for baseline comparison.

    Args:
        leaf_prs: List of PR numbers to sample from
        dag: Dependency DAG containing PR nodes (unused but kept for interface consistency)
        covered_files: Set of file paths already covered (unused but kept for interface consistency)
        seed: Random seed for deterministic sampling

    Returns:
        Randomly selected PR number
    """
    if seed is not None:
        random.seed(seed)

    return random.choice(leaf_prs)


@dataclass
class ChainValidationContext:
    """Context for validating a chain, reusing Docker container across candidates."""

    container: docker.models.containers.Container
    client: docker.DockerClient
    log_dir: Path
    validation_logger: logging.Logger
    applied_nodes: list[PRNode]

    def cleanup(self) -> None:
        """Clean up the Docker container."""
        if self.container:
            cleanup_container(self.client, self.container, self.validation_logger)


def create_validation_context(
    start_node: PRNode,
    client: docker.DockerClient,
    log_dir: Path,
) -> ChainValidationContext:
    """
    Create a validation context with a Docker container for a chain.

    Args:
        start_node: The first node in the chain (defines the base environment)
        client: Docker client for running validation
        log_dir: Directory for validation logs

    Returns:
        ChainValidationContext for the created container
    """
    # Create TestSpec for the start node (this defines the test environment)
    test_spec = make_test_spec(start_node.task_instance)

    log_file = log_dir / "validation.log"
    validation_logger = setup_logger(
        f"validate_chain_{start_node.instance_id}", log_file
    )

    validation_logger.info(
        f"Creating validation context for chain starting with PR {start_node.pr_number}"
    )

    container = build_container(
        test_spec,
        client,
        run_id="chain_validation",
        logger=validation_logger,
        nocache=False,
        force_rebuild=False,
    )
    container.start()
    validation_logger.info(f"Container started: {container.id}")

    return ChainValidationContext(
        container=container,
        client=client,
        log_dir=log_dir,
        validation_logger=validation_logger,
        applied_nodes=[],
    )


def validate_and_apply_candidate(
    context: ChainValidationContext,
    candidate: PRNode,
    timeout: int = 1800,
) -> bool:
    """
    Validate and apply a candidate PR to an existing validation context.

    This incrementally applies the candidate's patch to the existing container
    and verifies that FAIL_TO_PASS tests pass.

    Args:
        context: Validation context with Docker container
        candidate: PRNode to validate and apply
        timeout: Timeout for test execution in seconds

    Returns:
        True if candidate is valid (patch applies and tests pass), False otherwise
    """
    validation_logger = context.validation_logger
    container = context.container
    log_dir = context.log_dir

    validation_logger.info(
        f"Validating candidate {candidate.pr_number} "
        f"(chain length: {len(context.applied_nodes)})"
    )

    # Get the patch
    patch = candidate.task_instance.get("patch", "")
    if not patch or not patch.strip():
        validation_logger.warning(f"Candidate {candidate.pr_number} has empty patch")
        return False

    # Write patch to temporary file and copy to container
    patch_file = (
        log_dir / f"patch_{len(context.applied_nodes)}_{candidate.pr_number}.diff"
    )
    patch_file.write_text(patch)
    copy_to_container(container, patch_file, PurePosixPath(DOCKER_PATCH))

    # Try to apply patch
    validation_logger.info(f"Applying patch for PR {candidate.pr_number}")
    applied = False

    git_apply_cmds = [
        "git apply --verbose",
        "git apply --verbose --reject",
    ]

    for git_apply_cmd in git_apply_cmds:
        result = container.exec_run(
            f"{git_apply_cmd} {DOCKER_PATCH}",
            workdir=DOCKER_WORKDIR,
            user=DOCKER_USER,
        )
        if result.exit_code == 0:
            validation_logger.info(f"Patch applied successfully with: {git_apply_cmd}")
            applied = True
            break
        else:
            validation_logger.debug(
                f"Failed to apply with {git_apply_cmd}: {result.output.decode(UTF8)}"
            )

    if not applied:
        validation_logger.warning(f"Failed to apply patch for PR {candidate.pr_number}")
        return False

    # Run tests - only FAIL_TO_PASS tests for the candidate
    validation_logger.info(
        f"Running FAIL_TO_PASS tests for candidate {candidate.pr_number}"
    )

    # Create TestSpec for the candidate to get the right test configuration
    test_spec = make_test_spec(candidate.task_instance)

    # Create eval script
    eval_file = log_dir / f"eval_{len(context.applied_nodes)}_{candidate.pr_number}.sh"
    eval_file.write_text(test_spec.eval_script)
    copy_to_container(container, eval_file, PurePosixPath("/eval.sh"))

    # Execute tests
    test_output_file = (
        log_dir / f"test_output_{len(context.applied_nodes)}_{candidate.pr_number}.txt"
    )
    test_output, timed_out, total_runtime = exec_run_with_timeout(
        container, "/bin/bash /eval.sh", timeout
    )

    if timed_out:
        validation_logger.warning(f"Tests timed out after {timeout} seconds")
        return False

    # Write test output
    test_output_file.write_text(test_output)
    validation_logger.info(f"Tests completed in {total_runtime:.2f}s")

    # Parse test results
    status_map, tests_ran = get_logs_eval(test_spec, str(test_output_file))

    if not tests_ran:
        validation_logger.warning("Tests did not run successfully")
        return False

    # Check that all FAIL_TO_PASS tests passed
    fail_to_pass_tests = test_spec.FAIL_TO_PASS
    if not fail_to_pass_tests:
        validation_logger.warning("No FAIL_TO_PASS tests defined")
        return False

    all_passed = True
    for test_case in fail_to_pass_tests:
        test_status = status_map.get(test_case, "FAILED")
        if test_status not in ["PASSED", "XFAIL"]:
            validation_logger.warning(
                f"FAIL_TO_PASS test {test_case} did not pass: {test_status}"
            )
            all_passed = False

    if all_passed:
        validation_logger.info(
            f"All FAIL_TO_PASS tests passed for candidate {candidate.pr_number}"
        )
        # Add to applied nodes since validation succeeded
        context.applied_nodes.append(candidate)

    return all_passed


def sample_chains_from_dag(
    dag: DependencyDAG,
    num_chains: int = 10,
    min_chain_length: int = 2,
    max_chain_length: int = 1,
    sampler: PRSampler = file_coverage_sampler,
    seed: Optional[int] = None,
    validate_chains: bool = True,
    validation_timeout: int = 1800,
) -> list[list[Dict[str, Any]]]:
    """
    Sample diverse chains from the dependency DAG with optional validation.

    Args:
        dag: DependencyDAG to sample from
        num_chains: Number of chains to sample
        min_chain_length: Minimum chain length
        max_chain_length: Maximum chain length
        sampler: Function to select starting PR for each chain.
                 Takes (leaf_prs, dag, covered_files, seed) and returns selected PR number.
                 Defaults to file_coverage_sampler.
        seed: Random seed for deterministic sampling
        validate_chains: If True, validate that patches apply and tests pass
        validation_timeout: Timeout for test execution in validation (seconds)

    Returns:
        List of chains, where each chain is a list of task instances in dependency order
    """
    chains = []
    used_prs = set()
    covered_files = set()

    assert min_chain_length > 1, (
        f"Minimum chain length must be greater than 1, got {min_chain_length}"
    )

    # Get PRs in topological order (dependencies first)
    topo_order = dag.get_topological_order()

    # Filter to PRs with at least one edge (incoming or outgoing)
    # This excludes isolated nodes that can't form chains of min_chain_length >= 2
    connected_prs = [
        pr for pr in topo_order if dag.get_dependencies(pr) or dag.get_dependents(pr)
    ]

    logger.info(
        f"Chain sampling: {len(connected_prs)}/{len(topo_order)} PRs have at least one edge"
    )

    if not connected_prs:
        logger.warning("No connected PRs found in DAG - cannot sample chains")
        return []

    # Start from PRs with no dependencies (leaf nodes, oldest PRs) that are connected
    leaf_prs = [pr for pr in connected_prs if not dag.get_dependencies(pr)]

    # Initialize Docker client if validation is enabled
    docker_client = None
    if validate_chains:
        docker_client = docker.from_env()
        logger.info("Docker client initialized for chain validation")

    for i in range(num_chains):
        if not leaf_prs:
            logger.info(
                f"Stopped sampling after {i} chains - no more leaf PRs available"
            )
            break

        # Find a valid starting PR if validation is enabled
        validation_context = None
        if validate_chains:
            assert docker_client, (
                "Docker client must be provided when validate_chains=True"
            )

            # Try starting PRs until we find one that validates
            best_pr = None
            for _ in range(len(leaf_prs)):  # Avoid infinite loop
                candidate_pr = sampler(leaf_prs, dag, covered_files, seed)

                # Create validation context for this candidate
                temp_dir = tempfile.mkdtemp(prefix=f"chain_validation_{i}_")
                log_dir = Path(temp_dir)
                start_node = dag.nodes[candidate_pr]

                validation_context = create_validation_context(
                    start_node,
                    docker_client,
                    log_dir,
                )

                # Validate the starting node
                if validate_and_apply_candidate(
                    validation_context,
                    start_node,
                    validation_timeout,
                ):
                    best_pr = candidate_pr
                    logger.info(f"Starting PR {best_pr} validated successfully")
                    break
                else:
                    logger.debug(f"Starting PR {candidate_pr} failed validation")
                    validation_context.cleanup()
                    validation_context = None
                    # Remove failed candidate and try another
                    leaf_prs = [pr for pr in leaf_prs if pr != candidate_pr]
                    if not leaf_prs:
                        break

            if best_pr is None:
                logger.error("No valid starting PR found for chain validation")
                continue
        else:
            # No validation - just use sampler
            best_pr = sampler(leaf_prs, dag, covered_files, seed)

        # Build chain by following dependencies
        chain_nodes = []
        current_pr = best_pr

        try:
            while current_pr and len(chain_nodes) < max_chain_length:
                node = dag.nodes[current_pr]

                # Validate and apply current node if validation is enabled
                if validate_chains:
                    assert validation_context, (
                        "Validation context must exist when validate_chains=True"
                    )
                    # All nodes should validate (including first node for consistency)
                    success = validate_and_apply_candidate(
                        validation_context,
                        node,
                        validation_timeout,
                    )
                    assert success, f"Pre-validated node {current_pr} failed validation"

                # Add node to chain
                chain_nodes.append(node)
                used_prs.add(current_pr)
                covered_files.update(node.modified_files_pre | node.modified_files_post)

                # Find next node if we have room for more
                if len(chain_nodes) >= max_chain_length:
                    break

                dependent_weights = dag.get_dependent_weights(current_pr)
                if not dependent_weights:
                    # No more dependents, end chain
                    break

                if not validate_chains:
                    # No validation - just follow strongest dependent
                    best_dep = max(dependent_weights, key=dependent_weights.get)
                    current_pr = int(best_dep)
                    continue

                # With validation - find a valid dependent
                # Sort candidates by weight (descending)
                candidates = sorted(
                    dependent_weights.items(), key=lambda x: x[1], reverse=True
                )

                # Try candidates in order of weight
                validated_candidate = None
                for candidate_pr, weight in candidates:
                    candidate_node = dag.nodes[candidate_pr]
                    logger.debug(
                        f"Validating candidate {candidate_pr} "
                        f"(weight: {weight:.2f}) for chain position {len(chain_nodes)}"
                    )

                    # Validate this candidate as the next node in the chain
                    if validate_and_apply_candidate(
                        validation_context,
                        candidate_node,
                        validation_timeout,
                    ):
                        validated_candidate = candidate_pr
                        logger.info(f"Candidate {candidate_pr} validated successfully")
                        break
                    else:
                        logger.debug(f"Candidate {candidate_pr} failed validation")

                if validated_candidate is None:
                    logger.info(
                        f"Could not find valid candidate after {current_pr}, "
                        f"ending chain early at length {len(chain_nodes)}"
                    )
                    break

                # Use the validated candidate for next iteration
                current_pr = validated_candidate

            # Convert chain_nodes to task instances and add if meets minimum length
            chain = [node.task_instance for node in chain_nodes]
            if len(chain) >= min_chain_length:
                chains.append(chain)
                logger.info(
                    f"Sampled chain {len(chains)}: {[inst['pull_number'] for inst in chain]}"
                )
            else:
                logger.debug(
                    f"Chain from PR {best_pr} too short ({len(chain)} < {min_chain_length})"
                )
                # Remove this leaf PR so we don't try it again
                leaf_prs = [pr for pr in leaf_prs if pr != best_pr]

        finally:
            # Clean up validation context (Docker container) after chain is complete
            if validation_context:
                validation_context.cleanup()
                logger.info(f"Cleaned up validation context for chain {i + 1}")

    if not chains:
        logger.warning(
            f"Failed to sample any chains meeting min_chain_length={min_chain_length}"
        )
        logger.warning(
            f"  Total PRs: {len(dag.nodes)}, Connected PRs: {len(connected_prs)}, Leaf PRs: {len(leaf_prs)}"
        )

    return chains
