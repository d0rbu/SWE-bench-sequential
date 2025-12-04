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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

import unidiff
from tqdm import tqdm

logger = logging.getLogger(__name__)


# Type alias for PR sampler function
# Takes: (leaf_prs: List[int], dag: DependencyDAG, covered_files: Set[str], seed: Optional[int]) -> int
PRSampler = Callable[[List[int], "DependencyDAG", Set[str], Optional[int]], int]


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

    def get_dependencies(self, pr_number: int) -> List[int]:
        """Get PRs that this PR depends on."""
        return list(self.edges.get(pr_number, {}).keys())

    def get_dependents(self, pr_number: int) -> List[int]:
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

    def get_topological_order(self) -> List[int]:
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
    task_instances: List[Dict[str, Any]],
    time_window_months: int = 6,
    file_overlap_threshold: float = 0.0,
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
        file_overlap_threshold: Minimum file overlap weight for dependency (default: 0.0 = any overlap)

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
    leaf_prs: List[int],
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
    leaf_prs: List[int],
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


def sample_chains_from_dag(
    dag: DependencyDAG,
    num_chains: int = 10,
    min_chain_length: int = 2,
    max_chain_length: int = 5,
    sampler: PRSampler = file_coverage_sampler,
    seed: Optional[int] = None,
) -> List[List[Dict[str, Any]]]:
    """
    Sample diverse chains from the dependency DAG.

    Args:
        dag: DependencyDAG to sample from
        num_chains: Number of chains to sample
        min_chain_length: Minimum chain length
        max_chain_length: Maximum chain length
        sampler: Function to select starting PR for each chain.
                 Takes (leaf_prs, dag, covered_files, seed) and returns selected PR number.
                 Defaults to file_coverage_sampler.
        seed: Random seed for deterministic sampling

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

    # Start from PRs with no dependencies (leaf nodes in dep sense) that are connected
    leaf_prs = [pr for pr in connected_prs if not dag.get_dependencies(pr)]

    for i in range(num_chains):
        if not leaf_prs:
            logger.info(
                f"Stopped sampling after {i} chains - no more leaf PRs available"
            )
            break

        # Use the injected sampler to pick starting PR
        best_pr = sampler(leaf_prs, dag, covered_files, seed)

        # Build chain by following dependencies
        chain = []
        current_pr = best_pr

        while current_pr and len(chain) < max_chain_length:
            node = dag.nodes[current_pr]
            chain.append(node.task_instance)
            used_prs.add(current_pr)
            covered_files.update(node.modified_files_pre | node.modified_files_post)

            # Follow strongest dependent
            dependent_weights = dag.get_dependent_weights(current_pr)

            if not dependent_weights:
                break

            best_dep = max(dependent_weights, key=dependent_weights.get)
            current_pr: int = int(best_dep)

        # Only add if meets minimum length
        if len(chain) >= min_chain_length:
            # Reverse so it goes from oldest to newest (dependency order)
            chains.append(list(reversed(chain)))
            logger.info(
                f"Sampled chain {len(chains)}: {[inst['pull_number'] for inst in reversed(chain)]}"
            )
        else:
            logger.debug(
                f"Chain from PR {best_pr} too short ({len(chain)} < {min_chain_length})"
            )

    if not chains:
        logger.warning(
            f"Failed to sample any chains meeting min_chain_length={min_chain_length}"
        )
        logger.warning(
            f"  Total PRs: {len(dag.nodes)}, Connected PRs: {len(connected_prs)}, Leaf PRs: {len(leaf_prs)}"
        )

    return chains
