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
import time
from collections import Counter
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


def load_test_specs_from_dataset(
    dataset_name: str = "princeton-nlp/SWE-bench_Lite",
    split: str = "test"
) -> Dict[str, Dict[str, Any]]:
    """
    Load test specifications (FAIL_TO_PASS, PASS_TO_PASS) from official SWE-bench dataset.
    
    Args:
        dataset_name: Name of the dataset on HuggingFace (default: princeton-nlp/SWE-bench_Lite)
        split: Dataset split to load (default: test)
    
    Returns:
        Dictionary mapping instance_id to test specification fields (FAIL_TO_PASS, PASS_TO_PASS)
    """
    try:
        from datasets import load_dataset
    except ImportError:
        logger.warning(
            "datasets library not available. Cannot load test specifications from HuggingFace. "
            "Install with: pip install datasets"
        )
        return {}
    
    logger.info(f"Loading test specifications from {dataset_name} ({split} split)...")
    
    try:
        dataset = load_dataset(dataset_name, split=split)
        test_specs = {}
        
        for instance in dataset:
            instance_id = instance.get("instance_id")
            if not instance_id:
                continue
            
            test_specs[instance_id] = {
                "FAIL_TO_PASS": instance.get("FAIL_TO_PASS", "[]"),
                "PASS_TO_PASS": instance.get("PASS_TO_PASS", "[]"),
            }
        
        logger.info(f"Loaded test specifications for {len(test_specs)} instances")
        return test_specs
        
    except Exception as e:
        logger.error(f"Failed to load test specifications from {dataset_name}: {e}")
        return {}


def merge_test_specs_into_instances(
    task_instances: List[Dict[str, Any]],
    test_specs: Optional[Dict[str, Dict[str, Any]]] = None,
    dataset_name: Optional[str] = None,
    split: str = "test"
) -> List[Dict[str, Any]]:
    """
    Merge test specifications into task instances.
    
    Args:
        task_instances: List of task instance dictionaries
        test_specs: Pre-loaded test specifications. If None and dataset_name is provided,
                   will load from HuggingFace dataset.
        dataset_name: Optional dataset name to load test specs from if test_specs is None
        split: Dataset split to use if loading from HuggingFace
    
    Returns:
        Updated task instances with FAIL_TO_PASS and PASS_TO_PASS fields added
    """
    if test_specs is None and dataset_name:
        test_specs = load_test_specs_from_dataset(dataset_name, split)
    
    if not test_specs:
        logger.warning(
            "No test specifications provided. Instances will not have FAIL_TO_PASS/PASS_TO_PASS fields."
        )
        return task_instances
    
    merged_count = 0
    missing_count = 0
    
    for instance in task_instances:
        instance_id = instance.get("instance_id")
        if not instance_id:
            continue
        
        if instance_id in test_specs:
            # Only add if not already present
            if "FAIL_TO_PASS" not in instance:
                instance["FAIL_TO_PASS"] = test_specs[instance_id]["FAIL_TO_PASS"]
            if "PASS_TO_PASS" not in instance:
                instance["PASS_TO_PASS"] = test_specs[instance_id]["PASS_TO_PASS"]
            merged_count += 1
        else:
            missing_count += 1
    
    logger.info(f"Merged test specs for {merged_count} instances")
    if missing_count > 0:
        logger.warning(f"{missing_count} instances have no test specifications in the dataset")
    
    return task_instances


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


def convert_single_instances_to_chains(
    input_file: str, 
    output_file: str, 
    load_test_specs: bool = True,
    test_dataset: str = "princeton-nlp/SWE-bench_Lite",
    test_split: str = "test",
    **kwargs
) -> None:
    """
    Convert a file of single task instances to chains using DAG-based analysis.

    Supports both JSON and JSONL input formats. All task instance fields are
    preserved in the output chains, including version information if present.

    Args:
        input_file: Path to input file (JSON or JSONL) with single instances
        output_file: Path to output JSONL file with chains
        load_test_specs: If True, load FAIL_TO_PASS and PASS_TO_PASS tests from HuggingFace (default: True)
        test_dataset: Dataset name to load test specs from (default: princeton-nlp/SWE-bench_Lite)
        test_split: Dataset split to use (default: test)
        **kwargs: Additional arguments to pass to build_chains_from_repository_data

    Note:
        If task instances contain a 'version' field (e.g., from get_versions.py),
        this information is preserved in the chain output. The chain metadata
        does not explicitly track versions, but all original task instance
        fields are maintained.
        
        If load_test_specs is True, the function will attempt to load FAIL_TO_PASS
        and PASS_TO_PASS test specifications from the official SWE-bench dataset
        and merge them into the task instances. This is required for chain validation.
    """
    # Load single instances (supports both JSON and JSONL)
    task_instances = load_task_instances(input_file)
    
    # Load and merge test specifications if requested
    if load_test_specs:
        task_instances = merge_test_specs_into_instances(
            task_instances,
            dataset_name=test_dataset,
            split=test_split
        )

    # Build chains using DAG-based analysis
    # All task instance fields (including 'version') are preserved
    chains = build_chains_from_repository_data(task_instances, **kwargs)

    # Save chains
    save_chains_to_jsonl(chains, output_file)
