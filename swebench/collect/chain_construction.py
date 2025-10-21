"""
Chain construction module for SWE-bench-sequential.

This module provides the foundational chain construction functionality to handle
multi-turn chain logic, extending the existing single-turn task instance format
to support sequences of related PRs while maintaining backward compatibility.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
        metadata: Optional[Dict[str, Any]] = None
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
        self.metadata.update({
            "length": len(self.task_instances),
            "created_at": self.created_at,
            "validation_status": "pending",
            "repositories": list(set(
                instance.get("repo", "") for instance in self.task_instances
            )),
            "pull_numbers": [
                instance.get("pull_number") for instance in self.task_instances
            ],
            "date_range": self._get_date_range(),
            "dependencies": self._extract_dependencies()
        })
    
    def _get_date_range(self) -> Dict[str, Optional[str]]:
        """
        Get the date range covered by this chain.
        
        Returns:
            Dictionary with 'start' and 'end' dates
        """
        if not self.task_instances:
            return {"start": None, "end": None}
        
        dates = [
            instance.get("created_at") for instance in self.task_instances
            if instance.get("created_at")
        ]
        
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
        for i in range(1, len(self.task_instances)):
            prev_instance = self.task_instances[i-1]
            curr_instance = self.task_instances[i]
            
            dependencies.append({
                "type": "temporal",
                "from_pr": prev_instance.get("pull_number"),
                "to_pr": curr_instance.get("pull_number"),
                "relationship": "precedes"
            })
        
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
            instance.get("pull_number") for instance in self.task_instances
            if instance.get("pull_number") is not None
        ]
        if len(pr_numbers) != len(set(pr_numbers)):
            errors.append("Chain contains duplicate PR numbers")
        
        # Check that all instances have required fields
        required_fields = ["repo", "pull_number", "instance_id"]
        for i, instance in enumerate(self.task_instances):
            for field in required_fields:
                if field not in instance:
                    errors.append(f"Task instance {i} missing required field: {field}")
        
        # Check repository consistency (all instances should be from same repo)
        repos = set(instance.get("repo") for instance in self.task_instances)
        if len(repos) > 1:
            errors.append(f"Chain contains instances from multiple repositories: {repos}")
        
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
            instance for instance in self.task_instances
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
        for instance in self.task_instances:
            if instance.get("instance_id") == instance_id:
                return instance
        return None
    
    def sort_by_date(self, reverse: bool = False) -> None:
        """
        Sort task instances in the chain by creation date.
        
        Args:
            reverse: If True, sort in descending order (newest first)
        """
        self.task_instances.sort(
            key=lambda x: x.get("created_at", ""),
            reverse=reverse
        )
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
            "created_at": self.created_at
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
            metadata=data.get("metadata", {})
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


def create_chain_from_instances(
    task_instances: List[Dict[str, Any]],
    sort_by_date: bool = True
) -> Chain:
    """
    Create a chain from a list of task instances.
    
    Args:
        task_instances: List of task instance dictionaries
        sort_by_date: Whether to sort instances by creation date
        
    Returns:
        Chain object
    """
    chain = Chain(task_instances)
    
    if sort_by_date:
        chain.sort_by_date()
    
    return chain


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


def extend_task_instance_for_chain(
    task_instance: Dict[str, Any],
    chain_id: Optional[str] = None,
    chain_position: Optional[int] = None
) -> Dict[str, Any]:
    """
    Extend a task instance with chain-related fields.
    
    Args:
        task_instance: Original task instance dictionary
        chain_id: ID of the chain this instance belongs to
        chain_position: Position of this instance in the chain (0-based)
        
    Returns:
        Extended task instance dictionary
    """
    extended_instance = task_instance.copy()
    
    # Add chain-related fields
    extended_instance.update({
        "chain_id": chain_id,
        "chain_position": chain_position,
        "is_chain_member": chain_id is not None
    })
    
    return extended_instance


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
    grouping_strategy: str = "temporal",
    max_chain_length: int = 10,
    time_window_days: int = 30
) -> List[Chain]:
    """
    Build chains from a collection of task instances using various grouping strategies.
    
    Args:
        task_instances: List of task instance dictionaries
        grouping_strategy: Strategy for grouping instances ("temporal", "issue_based")
        max_chain_length: Maximum number of instances per chain
        time_window_days: Time window for temporal grouping (in days)
        
    Returns:
        List of Chain objects
    """
    if not task_instances:
        return []
    
    chains = []
    
    if grouping_strategy == "temporal":
        chains = _build_temporal_chains(task_instances, max_chain_length, time_window_days)
    elif grouping_strategy == "issue_based":
        chains = _build_issue_based_chains(task_instances, max_chain_length)
    else:
        # Default: treat each instance as a single-item chain
        chains = [create_single_instance_chain(instance) for instance in task_instances]
    
    return chains


def _build_temporal_chains(
    task_instances: List[Dict[str, Any]],
    max_chain_length: int,
    time_window_days: int
) -> List[Chain]:
    """
    Build chains based on temporal proximity of PRs.
    
    Args:
        task_instances: List of task instance dictionaries
        max_chain_length: Maximum number of instances per chain
        time_window_days: Time window for grouping (in days)
        
    Returns:
        List of Chain objects
    """
    from datetime import datetime, timedelta
    
    # Sort instances by creation date
    sorted_instances = sorted(
        task_instances,
        key=lambda x: x.get("created_at", "")
    )
    
    chains = []
    current_chain_instances = []
    
    for instance in sorted_instances:
        created_at_str = instance.get("created_at", "")
        if not created_at_str:
            # If no creation date, treat as single instance chain
            if current_chain_instances:
                chains.append(Chain(current_chain_instances))
                current_chain_instances = []
            chains.append(create_single_instance_chain(instance))
            continue
        
        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            # If date parsing fails, treat as single instance chain
            if current_chain_instances:
                chains.append(Chain(current_chain_instances))
                current_chain_instances = []
            chains.append(create_single_instance_chain(instance))
            continue
        
        # Check if this instance should be added to current chain
        should_add_to_current = False
        
        if current_chain_instances:
            last_instance = current_chain_instances[-1]
            last_created_str = last_instance.get("created_at", "")
            
            try:
                last_created = datetime.fromisoformat(last_created_str.replace("Z", "+00:00"))
                time_diff = created_at - last_created
                
                # Add to current chain if within time window and under max length
                if (time_diff <= timedelta(days=time_window_days) and 
                    len(current_chain_instances) < max_chain_length):
                    should_add_to_current = True
            except (ValueError, AttributeError):
                pass
        
        if should_add_to_current:
            current_chain_instances.append(instance)
        else:
            # Start new chain
            if current_chain_instances:
                chains.append(Chain(current_chain_instances))
            current_chain_instances = [instance]
    
    # Add final chain if exists
    if current_chain_instances:
        chains.append(Chain(current_chain_instances))
    
    return chains


def _build_issue_based_chains(
    task_instances: List[Dict[str, Any]],
    max_chain_length: int
) -> List[Chain]:
    """
    Build chains based on shared issues between PRs.
    
    Args:
        task_instances: List of task instance dictionaries
        max_chain_length: Maximum number of instances per chain
        
    Returns:
        List of Chain objects
    """
    # Group instances by shared issues
    issue_groups = {}
    
    for instance in task_instances:
        issue_numbers = instance.get("issue_numbers", [])
        if not issue_numbers:
            # No issues, treat as single instance chain
            continue
        
        # Use first issue as primary grouping key
        primary_issue = str(issue_numbers[0])
        if primary_issue not in issue_groups:
            issue_groups[primary_issue] = []
        issue_groups[primary_issue].append(instance)
    
    chains = []
    
    # Create chains from issue groups
    for issue_num, instances in issue_groups.items():
        # Sort by creation date
        instances.sort(key=lambda x: x.get("created_at", ""))
        
        # Split into chains if too long
        while instances:
            chain_instances = instances[:max_chain_length]
            instances = instances[max_chain_length:]
            
            if len(chain_instances) == 1:
                chains.append(create_single_instance_chain(chain_instances[0]))
            else:
                chains.append(Chain(chain_instances))
    
    return chains


def save_chains_to_jsonl(chains: List[Chain], output_file: str) -> None:
    """
    Save a list of chains to a JSONL file.
    
    Args:
        chains: List of Chain objects to save
        output_file: Path to output JSONL file
    """
    with open(output_file, 'w') as f:
        for chain in chains:
            f.write(chain.to_jsonl() + '\n')


def load_chains_from_jsonl(input_file: str) -> List[Chain]:
    """
    Load chains from a JSONL file.
    
    Args:
        input_file: Path to input JSONL file
        
    Returns:
        List of Chain objects
    """
    chains = []
    
    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                chain = Chain.from_jsonl(line)
                chains.append(chain)
    
    return chains


def convert_single_instances_to_chains(
    input_file: str,
    output_file: str,
    grouping_strategy: str = "temporal"
) -> None:
    """
    Convert a JSONL file of single task instances to chains.
    
    Args:
        input_file: Path to input JSONL file with single instances
        output_file: Path to output JSONL file with chains
        grouping_strategy: Strategy for grouping instances into chains
    """
    # Load single instances
    task_instances = []
    
    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                instance = json.loads(line)
                task_instances.append(instance)
    
    # Build chains
    chains = build_chains_from_repository_data(task_instances, grouping_strategy)
    
    # Save chains
    save_chains_to_jsonl(chains, output_file)
