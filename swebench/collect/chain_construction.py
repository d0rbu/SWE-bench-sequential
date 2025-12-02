"""
Chain construction module for SWE-bench-sequential.

This module provides DAG-based dependency analysis to construct chains of related PRs.
Dependencies are determined through git blame analysis on modified/deleted lines,
temporal proximity, issue relationships, and file overlap. Chains are sampled from
the resulting DAG to maximize diversity.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from itertools import pairwise
from typing import Any, Dict, List, Optional

# Import DAG-based dependency analysis
from swebench.collect.dag_dependency_analysis import (
    PRSampler,
    build_dependency_dag,
    file_coverage_sampler,
    sample_chains_from_dag,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import additional dependencies for FAIL_TO_PASS computation
import docker
import tempfile
from pathlib import Path
from swebench.harness.docker_build import build_container, setup_logger
from swebench.harness.docker_utils import cleanup_container, copy_to_container, exec_run_with_timeout
from swebench.harness.grading import get_logs_eval
from swebench.harness.test_spec.test_spec import make_test_spec
from swebench.harness.constants import DOCKER_PATCH, DOCKER_USER, DOCKER_WORKDIR, UTF8


class Chain:
    """
    Represents a sequence of related task instances forming a multi-turn chain.

    A chain contains multiple task instances that are related through shared issues,
    temporal proximity, or other relationships. Each chain has a unique ID and
    metadata describing its properties.
    """

    def __init__(
        self,
        task_instances: List[Dict[str, Any]],
        chain_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a Chain object.

        Args:
            task_instances: List of task instance dictionaries
            chain_id: Optional chain identifier. If None, will be auto-generated
            metadata: Optional metadata dictionary
        """
        self.task_instances = task_instances or []
        self.metadata = metadata or {}
        self.created_at = datetime.utcnow().isoformat()

        # Validate that all instances are from the same repository
        if self.task_instances:
            repos = set(instance.get("repo") for instance in self.task_instances)
            if len(repos) > 1:
                raise ValueError(
                    f"All task instances must be from the same repository. "
                    f"Found multiple repositories: {repos}"
                )

        # Generate chain ID if not provided
        if chain_id is None:
            self.chain_id = self._generate_chain_id()
        else:
            self.chain_id = chain_id

        # Update metadata with computed values
        self._update_metadata()

        # Validate the chain
        self._validate_chain()

    def _generate_chain_id(self) -> str:
        """
        Generate a unique chain ID based on constituent task instances.

        Format: {repo}__chain-{hash}
        where hash is derived from PR numbers and timestamps.

        Returns:
            Unique chain identifier string
        """
        if not self.task_instances:
            # Fallback for empty chains
            timestamp = str(int(time.time()))
            return f"empty__chain-{timestamp}"

        # Get repo name from first task instance
        repo = self.task_instances[0].get("repo", "unknown")
        repo_clean = repo.replace("/", "__")

        # Create hash from PR numbers and creation times
        hash_input = ""
        for instance in self.task_instances:
            pr_num = instance.get("pull_number", 0)
            created = instance.get("created_at", "")
            hash_input += f"{pr_num}-{created}"

        # Generate short hash
        hash_obj = hashlib.md5(hash_input.encode())
        short_hash = hash_obj.hexdigest()[:8]

        return f"{repo_clean}__chain-{short_hash}"

    def _update_metadata(self) -> None:
        """Update chain metadata with computed values."""
        # Extract PR numbers, filtering out None values
        raw_pr_numbers = [
            instance.get("pull_number") for instance in self.task_instances
        ]
        pr_numbers = [pr_num for pr_num in raw_pr_numbers if pr_num is not None]

        self.metadata.update(
            {
                "length": len(self.task_instances),
                "created_at": self.created_at,
                "validation_status": "pending",
                "repositories": list(
                    set(instance.get("repo", "") for instance in self.task_instances)
                ),
                "pull_numbers": pr_numbers,
                "date_range": self._get_date_range(),
                "dependencies": self._extract_dependencies(),
            }
        )

    def _get_date_range(self) -> Dict[str, Optional[str]]:
        """
        Get the date range covered by this chain.

        Returns:
            Dictionary with 'start' and 'end' dates
        """
        if not self.task_instances:
            return {"start": None, "end": None}

        # Extract dates, filtering out None values
        raw_dates = [instance.get("created_at") for instance in self.task_instances]
        dates = [date for date in raw_dates if date is not None]

        if not dates:
            return {"start": None, "end": None}

        dates.sort()
        return {"start": dates[0], "end": dates[-1]}

    def _extract_dependencies(self) -> List[Dict[str, Any]]:
        """
        Extract dependency relationships between task instances in the chain.

        Returns:
            List of dependency relationships
        """
        dependencies = []

        # Simple temporal dependencies (each PR depends on the previous one)
        for prev_instance, curr_instance in pairwise(self.task_instances):
            dependencies.append(
                {
                    "from_pr": prev_instance.get("pull_number"),
                    "to_pr": curr_instance.get("pull_number"),
                }
            )

        return dependencies

    def _validate_chain(self) -> None:
        """
        Validate the chain for integrity and consistency.

        Raises:
            ValueError: If chain validation fails
        """
        errors = []

        # Check for empty chain
        if not self.task_instances:
            errors.append("Chain cannot be empty")

        # Check for duplicate PRs
        pr_numbers = [
            instance.get("pull_number")
            for instance in self.task_instances
            if instance.get("pull_number") is not None
        ]
        # Use Counter to find duplicates and their counts
        pr_counts = Counter(pr_numbers)
        duplicates = {pr: count for pr, count in pr_counts.items() if count > 1}
        if duplicates:
            errors.append(
                f"Chain contains duplicate PR numbers: {duplicates} (PR number: count)"
            )

        # Check that all instances have required fields
        required_fields = ["repo", "pull_number", "instance_id"]
        for i, instance in enumerate(self.task_instances):
            for field in required_fields:
                if field not in instance:
                    errors.append(f"Task instance {i} missing required field: {field}")

        # Check repository consistency (all instances should be from same repo)
        repos = set(instance.get("repo") for instance in self.task_instances)
        if len(repos) > 1:
            errors.append(
                f"Chain contains instances from multiple repositories: {repos}"
            )

        if errors:
            self.metadata["validation_status"] = "failed"
            self.metadata["validation_errors"] = errors
            raise ValueError(f"Chain validation failed: {'; '.join(errors)}")
        else:
            self.metadata["validation_status"] = "passed"

    def add_task_instance(self, task_instance: Dict[str, Any]) -> None:
        """
        Add a task instance to the chain.

        Args:
            task_instance: Task instance dictionary to add
        """
        self.task_instances.append(task_instance)
        self._update_metadata()
        self._validate_chain()

    def remove_task_instance(self, instance_id: str) -> bool:
        """
        Remove a task instance from the chain by instance ID.

        Args:
            instance_id: ID of the task instance to remove

        Returns:
            True if instance was found and removed, False otherwise
        """
        original_length = len(self.task_instances)
        self.task_instances = [
            instance
            for instance in self.task_instances
            if instance.get("instance_id") != instance_id
        ]

        if len(self.task_instances) < original_length:
            self._update_metadata()
            if self.task_instances:  # Only validate if chain is not empty
                self._validate_chain()
            return True
        return False

    def get_task_instance(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a task instance by its ID.

        Args:
            instance_id: ID of the task instance to retrieve

        Returns:
            Task instance dictionary if found, None otherwise
        """
        matching_instances = [
            instance
            for instance in self.task_instances
            if instance.get("instance_id") == instance_id
        ]

        if len(matching_instances) == 0:
            return None

        assert len(matching_instances) == 1, (
            f"Expected exactly 1 instance with ID '{instance_id}', "
            f"found {len(matching_instances)}"
        )

        return matching_instances[0]

    def sort_by_date(self, reverse: bool = False) -> None:
        """
        Sort task instances in the chain by creation date.

        Args:
            reverse: If True, sort in descending order (newest first)
        """
        self.task_instances.sort(key=lambda x: x.get("created_at", ""), reverse=reverse)
        self._update_metadata()

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert chain to dictionary representation.

        Returns:
            Dictionary representation of the chain
        """
        return {
            "chain_id": self.chain_id,
            "task_instances": self.task_instances,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }

    def to_jsonl(self) -> str:
        """
        Serialize chain to JSONL format.

        Returns:
            JSONL string representation of the chain
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Chain:
        """
        Create Chain object from dictionary representation.

        Args:
            data: Dictionary containing chain data

        Returns:
            Chain object
        """
        chain = cls(
            task_instances=data.get("task_instances", []),
            chain_id=data.get("chain_id"),
            metadata=data.get("metadata", {}),
        )

        # Restore created_at if provided
        if "created_at" in data:
            chain.created_at = data["created_at"]

        return chain

    @classmethod
    def from_jsonl(cls, jsonl_str: str) -> Chain:
        """
        Deserialize chain from JSONL format.

        Args:
            jsonl_str: JSONL string representation

        Returns:
            Chain object
        """
        data = json.loads(jsonl_str)
        return cls.from_dict(data)

    def __len__(self) -> int:
        """Return the number of task instances in the chain."""
        return len(self.task_instances)

    def __iter__(self):
        """Iterate over task instances in the chain."""
        return iter(self.task_instances)

    def __getitem__(self, index: int) -> Dict[str, Any]:
        """Get task instance by index."""
        return self.task_instances[index]

    def __repr__(self) -> str:
        """String representation of the chain."""
        return f"Chain(id='{self.chain_id}', length={len(self.task_instances)})"


def create_single_instance_chain(task_instance: Dict[str, Any]) -> Chain:
    """
    Create a chain containing a single task instance.

    This function provides backward compatibility by treating single instances
    as single-item chains.

    Args:
        task_instance: Single task instance dictionary

    Returns:
        Chain object containing the single instance
    """
    return Chain([task_instance])


def validate_chain_id(chain_id: str) -> bool:
    """
    Validate that a chain ID follows the expected format.

    Args:
        chain_id: Chain ID to validate

    Returns:
        True if valid, False otherwise
    """
    if not chain_id:
        return False

    # Expected format: {repo}__chain-{hash}
    parts = chain_id.split("__chain-")
    if len(parts) != 2:
        return False

    repo_part, hash_part = parts

    # Check that repo part is not empty and hash part is reasonable length
    if not repo_part or len(hash_part) < 4:
        return False

    return True


def build_chains_from_repository_data(
    task_instances: List[Dict[str, Any]],
    time_window_months: int = 6,
    file_overlap_threshold: float = 0.0,
    num_chains: int = 10,
    min_chain_length: int = 2,
    max_chain_length: int = 5,
    sampler: PRSampler = file_coverage_sampler,
    seed: Optional[int] = None,
) -> List[Chain]:
    """
    Build chains from task instances using DAG-based dependency analysis.

    This performs sophisticated dependency detection through:
    - File overlap detection (with precise pre/post state tracking)
    - Temporal proximity filtering
    - Issue relationship matching

    Args:
        task_instances: List of task instance dictionaries
        time_window_months: Maximum age difference for dependencies (default: 6)
        file_overlap_threshold: Minimum file overlap weight for dependency (default: 0.0 = any overlap)
        num_chains: Number of chains to sample from DAG
        min_chain_length: Minimum chain length
        max_chain_length: Maximum chain length
        sampler: Function to select starting PR for chains. Takes (leaf_prs, dag, covered_files, seed)
                 and returns selected PR number. Defaults to file_coverage_sampler.
        seed: Random seed for deterministic sampling

    Returns:
        List of Chain objects sampled from the dependency DAG
    """
    if not task_instances:
        return []

    logger.info(f"Building dependency DAG from {len(task_instances)} task instances")

    # Build the dependency DAG
    dag = build_dependency_dag(
        task_instances,
        time_window_months=time_window_months,
        file_overlap_threshold=file_overlap_threshold,
    )

    logger.info(
        f"DAG built with {len(dag.nodes)} nodes and "
        f"{sum(len(deps) for deps in dag.edges.values())} edges"
    )

    # Sample diverse chains from the DAG
    chain_instances = sample_chains_from_dag(
        dag,
        num_chains=num_chains,
        min_chain_length=min_chain_length,
        max_chain_length=max_chain_length,
        sampler=sampler,
        seed=seed,
    )

    # Convert to Chain objects
    chains = [Chain(instances) for instances in chain_instances]

    logger.info(f"Sampled {len(chains)} diverse chains from DAG")
    return chains


def save_chains_to_jsonl(chains: List[Chain], output_file: str) -> None:
    """
    Save a list of chains to a JSONL file.

    Args:
        chains: List of Chain objects to save
        output_file: Path to output JSONL file
    """
    with open(output_file, "w") as f:
        for chain in chains:
            f.write(chain.to_jsonl() + "\n")


def load_chains_from_jsonl(input_file: str) -> List[Chain]:
    """
    Load chains from a JSONL file.

    Args:
        input_file: Path to input JSONL file

    Returns:
        List of Chain objects
    """
    chains = []

    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                chain = Chain.from_jsonl(line)
                chains.append(chain)

    return chains


def load_task_instances(input_file: str) -> List[Dict[str, Any]]:
    """
    Load task instances from a file, supporting both JSON and JSONL formats.

    The format is auto-detected based on file extension:
    - .jsonl or .jsonl.all: JSONL format (one JSON object per line)
    - .json or other: JSON format (single array of objects)

    All fields from the input instances are preserved, including:
    - version: Version information added by get_versions.py
    - repo, pull_number, instance_id, base_commit, patch, etc.

    Args:
        input_file: Path to input file (JSON or JSONL)

    Returns:
        List of task instance dictionaries with all fields preserved
    """
    task_instances = []

    if any(input_file.endswith(ext) for ext in [".jsonl", ".jsonl.all"]):
        # JSONL format
        with open(input_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    instance = json.loads(line)
                    task_instances.append(instance)
    else:
        # JSON format (single array)
        with open(input_file, "r") as f:
            task_instances = json.load(f)

    return task_instances


def compute_fail_to_pass_for_instance(
    instance: Dict[str, Any],
    client: Optional[docker.DockerClient] = None,
    timeout: int = 1800,
) -> Dict[str, Any]:
    """
    Compute FAIL_TO_PASS test list for a task instance by running tests without the fix.
    
    This function:
    1. Creates a test environment at the base commit
    2. Applies only the test_patch (not the main patch)
    3. Runs tests and identifies which ones fail
    4. Those failing tests become FAIL_TO_PASS
    
    Args:
        instance: Task instance dictionary containing test_patch, patch, etc.
        client: Docker client (creates new one if not provided)
        timeout: Timeout for test execution in seconds
        
    Returns:
        Updated instance with FAIL_TO_PASS and PASS_TO_PASS fields added
    """
    # Skip if already has FAIL_TO_PASS
    if "FAIL_TO_PASS" in instance and instance["FAIL_TO_PASS"]:
        logger.debug(f"Instance {instance.get('instance_id')} already has FAIL_TO_PASS")
        return instance
    
    # Skip if no test_patch
    test_patch = instance.get("test_patch", "")
    if not test_patch or not test_patch.strip():
        logger.warning(f"Instance {instance.get('instance_id')} has no test_patch, cannot compute FAIL_TO_PASS")
        instance["FAIL_TO_PASS"] = []
        instance["PASS_TO_PASS"] = []
        return instance
    
    logger.info(f"Computing FAIL_TO_PASS for instance {instance.get('instance_id')}")
    
    # Create Docker client if not provided
    if client is None:
        client = docker.from_env()
    
    # Create test spec (will determine test environment)
    test_spec = make_test_spec(instance)
    
    # Create temporary log directory
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)
        log_file = log_dir / "compute_fail_to_pass.log"
        compute_logger = setup_logger(
            f"compute_f2p_{instance.get('instance_id')}",
            log_file
        )
        
        container = None
        try:
            # Build and start container
            compute_logger.info("Building container for FAIL_TO_PASS computation")
            container = build_container(
                test_spec,
                client,
                run_id="compute_fail_to_pass",
                logger=compute_logger,
                nocache=False,
                force_rebuild=False,
            )
            container.start()
            compute_logger.info(f"Container started: {container.id}")
            
            # Apply test_patch to the container
            compute_logger.info("Applying test_patch to container")
            test_patch_file = log_dir / "test.patch"
            test_patch_file.write_text(test_patch)
            copy_to_container(container, test_patch_file, Path(DOCKER_PATCH))
            
            # Try to apply test patch
            result = container.exec_run(
                f"git apply --verbose {DOCKER_PATCH}",
                workdir=DOCKER_WORKDIR,
                user=DOCKER_USER,
            )
            
            if result.exit_code != 0:
                compute_logger.warning(
                    f"Failed to apply test_patch: {result.output.decode(UTF8)}"
                )
                # If test patch can't apply, we can't determine FAIL_TO_PASS
                instance["FAIL_TO_PASS"] = []
                instance["PASS_TO_PASS"] = []
                return instance
            
            compute_logger.info("Test patch applied successfully")
            
            # Run tests
            compute_logger.info("Running tests to identify FAIL_TO_PASS")
            eval_file = log_dir / "eval.sh"
            eval_file.write_text(test_spec.eval_script)
            copy_to_container(container, eval_file, Path("/eval.sh"))
            
            test_output, timed_out, total_runtime = exec_run_with_timeout(
                container, "/bin/bash /eval.sh", timeout
            )
            
            if timed_out:
                compute_logger.warning(f"Tests timed out after {timeout} seconds")
                instance["FAIL_TO_PASS"] = []
                instance["PASS_TO_PASS"] = []
                return instance
            
            # Write test output and parse results
            test_output_file = log_dir / "test_output.txt"
            test_output_file.write_text(test_output)
            compute_logger.info(f"Tests completed in {total_runtime:.2f}s")
            
            # Parse test results
            status_map, tests_ran = get_logs_eval(test_spec, str(test_output_file))
            
            if not tests_ran:
                compute_logger.warning("Tests did not run successfully")
                instance["FAIL_TO_PASS"] = []
                instance["PASS_TO_PASS"] = []
                return instance
            
            # Tests that FAIL are FAIL_TO_PASS (they should pass after fix)
            # Tests that PASS are PASS_TO_PASS (they should still pass after fix)
            fail_to_pass = []
            pass_to_pass = []
            
            for test_case, status in status_map.items():
                if status in ["PASSED", "XFAIL"]:
                    pass_to_pass.append(test_case)
                else:
                    fail_to_pass.append(test_case)
            
            compute_logger.info(
                f"Found {len(fail_to_pass)} FAIL_TO_PASS tests and "
                f"{len(pass_to_pass)} PASS_TO_PASS tests"
            )
            
            # Store in instance (as JSON strings to match dataset format)
            instance["FAIL_TO_PASS"] = json.dumps(fail_to_pass)
            instance["PASS_TO_PASS"] = json.dumps(pass_to_pass)
            
            return instance
            
        except Exception as e:
            compute_logger.error(f"Error computing FAIL_TO_PASS: {e}", exc_info=True)
            instance["FAIL_TO_PASS"] = []
            instance["PASS_TO_PASS"] = []
            return instance
            
        finally:
            # Always cleanup container
            if container:
                try:
                    cleanup_container(client, container, compute_logger)
                except Exception as e:
                    compute_logger.warning(f"Error cleaning up container: {e}")


def compute_fail_to_pass_for_instances(
    instances: List[Dict[str, Any]],
    max_workers: int = 5,
    timeout: int = 1800,
) -> List[Dict[str, Any]]:
    """
    Compute FAIL_TO_PASS for multiple instances.
    
    Args:
        instances: List of task instances
        max_workers: Number of parallel workers (default: 5)
        timeout: Timeout per instance in seconds
        
    Returns:
        Updated instances with FAIL_TO_PASS computed
    """
    # Suppress verbose logging from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("docker").setLevel(logging.WARNING)
    logging.getLogger("docker.utils.config").setLevel(logging.WARNING)
    logging.getLogger("docker.auth").setLevel(logging.WARNING)
    
    logger.info(f"Computing FAIL_TO_PASS for {len(instances)} instances with {max_workers} workers")
    
    # Track statistics
    success_count = 0
    failed_count = 0
    skipped_count = 0
    stats_lock = threading.Lock()
    
    if max_workers == 1:
        # Sequential processing for compatibility
        client = docker.from_env()
        updated_instances = []
        for i, instance in enumerate(instances, 1):
            instance_id = instance.get('instance_id')
            logger.info(f"[{i}/{len(instances)}] Processing {instance_id}")
            
            had_fail_to_pass_before = bool(instance.get("FAIL_TO_PASS"))
            updated_instance = compute_fail_to_pass_for_instance(instance, client, timeout)
            updated_instances.append(updated_instance)
            
            # Update statistics
            if had_fail_to_pass_before:
                skipped_count += 1
                logger.info(f"  ⏭️  Skipped (already had FAIL_TO_PASS)")
            elif updated_instance.get("FAIL_TO_PASS"):
                fail_to_pass = updated_instance.get("FAIL_TO_PASS")
                if isinstance(fail_to_pass, str):
                    fail_to_pass = json.loads(fail_to_pass) if fail_to_pass else []
                
                if fail_to_pass:
                    success_count += 1
                    logger.info(f"  ✅ Success ({len(fail_to_pass)} FAIL_TO_PASS tests)")
                else:
                    pass_to_pass = updated_instance.get("PASS_TO_PASS")
                    if isinstance(pass_to_pass, str):
                        pass_to_pass = json.loads(pass_to_pass) if pass_to_pass else []
                    
                    if pass_to_pass:
                        success_count += 1
                        logger.info(f"  ✅ Success (0 FAIL_TO_PASS, {len(pass_to_pass)} PASS_TO_PASS)")
                    else:
                        failed_count += 1
                        logger.warning(f"  ❌ Failed (no tests found)")
            else:
                failed_count += 1
                logger.warning(f"  ❌ Failed (computation error)")
            
            # Progress update every 10 instances
            if i % 10 == 0:
                logger.info(
                    f"Progress: {i}/{len(instances)} | "
                    f"✅ {success_count} | ❌ {failed_count} | ⏭️  {skipped_count}"
                )
        
        # Final summary
        logger.info("=" * 60)
        logger.info("FAIL_TO_PASS Computation Summary:")
        logger.info(f"  Total:      {len(instances)}")
        logger.info(f"  ✅ Success:  {success_count}")
        logger.info(f"  ❌ Failed:   {failed_count}")
        logger.info(f"  ⏭️  Skipped:  {skipped_count}")
        if len(instances) > 0:
            logger.info(f"  Success rate: {success_count / len(instances) * 100:.1f}%")
        logger.info("=" * 60)
        
        return updated_instances
    
    # Parallel processing with ThreadPoolExecutor
    def process_instance_worker(instance_with_index):
        """Worker function that processes a single instance with its own Docker client."""
        nonlocal success_count, failed_count, skipped_count
        
        i, instance = instance_with_index
        instance_id = instance.get('instance_id', f'instance_{i}')
        
        # Each worker gets its own Docker client to avoid conflicts
        client = docker.from_env()
        try:
            had_fail_to_pass_before = bool(instance.get("FAIL_TO_PASS"))
            updated_instance = compute_fail_to_pass_for_instance(instance, client, timeout)
            
            # Update statistics (thread-safe)
            with stats_lock:
                if had_fail_to_pass_before:
                    skipped_count += 1
                elif updated_instance.get("FAIL_TO_PASS"):
                    fail_to_pass = updated_instance.get("FAIL_TO_PASS")
                    if isinstance(fail_to_pass, str):
                        fail_to_pass = json.loads(fail_to_pass) if fail_to_pass else []
                    
                    if fail_to_pass or updated_instance.get("PASS_TO_PASS"):
                        success_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
            
            return i, updated_instance
        except Exception as e:
            logger.error(f"Failed to process instance {i+1}/{len(instances)} ({instance_id}): {e}")
            with stats_lock:
                failed_count += 1
            # Return original instance if processing fails
            return i, instance
    
    # Optimize worker count based on actual number of instances
    actual_workers = min(max_workers, len(instances))
    
    # Process instances in parallel
    updated_instances = [None] * len(instances)  # Pre-allocate list with correct order
    
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(process_instance_worker, (i, instance)): i 
            for i, instance in enumerate(instances)
        }
        
        # Collect results as they complete
        completed_count = 0
        for future in as_completed(futures):
            try:
                i, updated_instance = future.result()
                updated_instances[i] = updated_instance
                completed_count += 1
                
                # Progress update every 10 completions
                if completed_count % 10 == 0:
                    with stats_lock:
                        logger.info(
                            f"Progress: {completed_count}/{len(instances)} | "
                            f"✅ {success_count} | ❌ {failed_count} | ⏭️  {skipped_count}"
                        )
            except Exception as e:
                logger.error(f"Worker thread failed with error: {e}")
                # The worker already handles exceptions and returns original instance
    
    # Final summary
    logger.info("=" * 60)
    logger.info("FAIL_TO_PASS Computation Summary:")
    logger.info(f"  Total:      {len(instances)}")
    logger.info(f"  ✅ Success:  {success_count}")
    logger.info(f"  ❌ Failed:   {failed_count}")
    logger.info(f"  ⏭️  Skipped:  {skipped_count}")
    if len(instances) > 0:
        logger.info(f"  Success rate: {success_count / len(instances) * 100:.1f}%")
    logger.info("=" * 60)
    
    return updated_instances


def convert_single_instances_to_chains(
    input_file: str, 
    output_file: str, 
    compute_fail_to_pass: bool = True,
    fail_to_pass_timeout: int = 1800,
    max_workers: int = 5,
    **kwargs
) -> None:
    """
    Convert a file of single task instances to chains using DAG-based analysis.

    Supports both JSON and JSONL input formats. All task instance fields are
    preserved in the output chains, including version information if present.

    Args:
        input_file: Path to input file (JSON or JSONL) with single instances
        output_file: Path to output JSONL file with chains
        compute_fail_to_pass: If True, compute FAIL_TO_PASS by running tests (default: True)
        fail_to_pass_timeout: Timeout for FAIL_TO_PASS computation per instance in seconds (default: 1800)
        max_workers: Number of parallel workers for FAIL_TO_PASS computation (default: 5)
        **kwargs: Additional arguments to pass to build_chains_from_repository_data

    Note:
        If task instances contain a 'version' field (e.g., from get_versions.py),
        this information is preserved in the chain output. The chain metadata
        does not explicitly track versions, but all original task instance
        fields are maintained.
        
        If compute_fail_to_pass is True, the function will compute FAIL_TO_PASS
        and PASS_TO_PASS test lists by running tests without the fix patch.
        This is required for chain validation during sampling.
    """
    # Load single instances (supports both JSON and JSONL)
    task_instances = load_task_instances(input_file)
    
    # Compute FAIL_TO_PASS if requested
    if compute_fail_to_pass:
        logger.info("Computing FAIL_TO_PASS for task instances")
        task_instances = compute_fail_to_pass_for_instances(
            task_instances,
            max_workers=max_workers,
            timeout=fail_to_pass_timeout
        )

    # Build chains using DAG-based analysis
    # All task instance fields (including 'version') are preserved
    chains = build_chains_from_repository_data(task_instances, **kwargs)

    # Save chains
    save_chains_to_jsonl(chains, output_file)
