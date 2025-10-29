"""
Unit tests for the chain construction module.

Tests cover Chain class functionality, serialization/deserialization,
chain ID generation, metadata calculation, validation, and integration
with existing task instance format.
"""

import json
import pytest
import tempfile
import os
from datetime import datetime, timedelta

from swebench.collect.chain_construction import (
    Chain,
    create_single_instance_chain,
    validate_chain_id,
    build_chains_from_repository_data,
    save_chains_to_jsonl,
    load_chains_from_jsonl,
    convert_single_instances_to_chains,
)


class TestChain:
    """Test cases for the Chain class."""
    
    def test_chain_initialization_empty(self):
        """Test Chain initialization with empty task instances."""
        with pytest.raises(ValueError, match="Chain cannot be empty"):
            Chain([])
    
    def test_chain_initialization_valid(self):
        """Test Chain initialization with valid task instances."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            }
        ]
        
        chain = Chain(task_instances)
        
        assert chain.chain_id.startswith("test__repo__chain-")
        assert len(chain) == 1
        assert chain.metadata["length"] == 1
        assert chain.metadata["validation_status"] == "passed"
    
    def test_chain_id_generation(self):
        """Test automatic chain ID generation."""
        task_instances = [
            {
                "repo": "owner/repo",
                "pull_number": 123,
                "instance_id": "owner__repo-123",
                "created_at": "2023-01-01T00:00:00Z"
            },
            {
                "repo": "owner/repo", 
                "pull_number": 124,
                "instance_id": "owner__repo-124",
                "created_at": "2023-01-02T00:00:00Z"
            }
        ]
        
        chain = Chain(task_instances)
        
        # Should follow format: owner__repo__chain-{hash}
        assert chain.chain_id.startswith("owner__repo__chain-")
        assert len(chain.chain_id.split("__chain-")[1]) == 8  # 8-char hash
    
    def test_chain_validation_duplicate_prs(self):
        """Test chain validation fails with duplicate PR numbers."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            },
            {
                "repo": "test/repo",
                "pull_number": 1,  # Duplicate PR number
                "instance_id": "test__repo-1-dup",
                "created_at": "2023-01-02T00:00:00Z"
            }
        ]
        
        with pytest.raises(ValueError, match="duplicate PR numbers.*1.*2"):
            Chain(task_instances)
    
    def test_chain_validation_multiple_repos(self):
        """Test chain validation fails with multiple repositories."""
        task_instances = [
            {
                "repo": "test/repo1",
                "pull_number": 1,
                "instance_id": "test__repo1-1",
                "created_at": "2023-01-01T00:00:00Z"
            },
            {
                "repo": "test/repo2",  # Different repo
                "pull_number": 2,
                "instance_id": "test__repo2-2",
                "created_at": "2023-01-02T00:00:00Z"
            }
        ]
        
        with pytest.raises(ValueError, match="multiple repositories"):
            Chain(task_instances)
    
    def test_chain_validation_missing_fields(self):
        """Test chain validation fails with missing required fields."""
        task_instances = [
            {
                "repo": "test/repo",
                # Missing pull_number and instance_id
                "created_at": "2023-01-01T00:00:00Z"
            }
        ]
        
        with pytest.raises(ValueError, match="missing required field"):
            Chain(task_instances)
    
    def test_chain_metadata_calculation(self):
        """Test chain metadata is calculated correctly."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            },
            {
                "repo": "test/repo",
                "pull_number": 2,
                "instance_id": "test__repo-2",
                "created_at": "2023-01-02T00:00:00Z"
            }
        ]
        
        chain = Chain(task_instances)
        
        assert chain.metadata["length"] == 2
        assert chain.metadata["repositories"] == ["test/repo"]
        assert chain.metadata["pull_numbers"] == [1, 2]
        assert chain.metadata["date_range"]["start"] == "2023-01-01T00:00:00Z"
        assert chain.metadata["date_range"]["end"] == "2023-01-02T00:00:00Z"
        assert len(chain.metadata["dependencies"]) == 1  # One temporal dependency
    
    def test_chain_add_task_instance(self):
        """Test adding task instance to chain."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            }
        ]
        
        chain = Chain(task_instances)
        original_length = len(chain)
        
        new_instance = {
            "repo": "test/repo",
            "pull_number": 2,
            "instance_id": "test__repo-2",
            "created_at": "2023-01-02T00:00:00Z"
        }
        
        chain.add_task_instance(new_instance)
        
        assert len(chain) == original_length + 1
        assert chain.metadata["length"] == 2
    
    def test_chain_remove_task_instance(self):
        """Test removing task instance from chain."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            },
            {
                "repo": "test/repo",
                "pull_number": 2,
                "instance_id": "test__repo-2",
                "created_at": "2023-01-02T00:00:00Z"
            }
        ]
        
        chain = Chain(task_instances)
        
        # Remove first instance
        removed = chain.remove_task_instance("test__repo-1")
        
        assert removed is True
        assert len(chain) == 1
        assert chain.get_task_instance("test__repo-1") is None
        assert chain.get_task_instance("test__repo-2") is not None
    
    def test_chain_sort_by_date(self):
        """Test sorting chain by date."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 2,
                "instance_id": "test__repo-2",
                "created_at": "2023-01-02T00:00:00Z"
            },
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            }
        ]
        
        chain = Chain(task_instances)
        chain.sort_by_date()
        
        # Should be sorted by date (ascending)
        assert chain[0]["pull_number"] == 1
        assert chain[1]["pull_number"] == 2
    
    def test_chain_serialization_roundtrip(self):
        """Test chain serialization and deserialization."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z",
                "patch": "test patch",
                "problem_statement": "test problem"
            }
        ]
        
        original_chain = Chain(task_instances)
        
        # Serialize to JSONL
        jsonl_str = original_chain.to_jsonl()
        
        # Deserialize back
        restored_chain = Chain.from_jsonl(jsonl_str)
        
        assert restored_chain.chain_id == original_chain.chain_id
        assert len(restored_chain) == len(original_chain)
        assert restored_chain[0]["repo"] == original_chain[0]["repo"]
        assert restored_chain[0]["pull_number"] == original_chain[0]["pull_number"]
        assert restored_chain.metadata["length"] == original_chain.metadata["length"]


class TestChainUtilities:
    """Test cases for chain utility functions."""
    
    def test_create_single_instance_chain(self):
        """Test creating a single-instance chain."""
        task_instance = {
            "repo": "test/repo",
            "pull_number": 1,
            "instance_id": "test__repo-1",
            "created_at": "2023-01-01T00:00:00Z"
        }
        
        chain = create_single_instance_chain(task_instance)
        
        assert len(chain) == 1
        assert chain[0] == task_instance
        assert chain.metadata["validation_status"] == "passed"
    
    def test_validate_chain_id_valid(self):
        """Test chain ID validation with valid IDs."""
        valid_ids = [
            "owner__repo__chain-12345678",
            "test__project__chain-abcd1234",
            "org__name__chain-xyz98765"
        ]
        
        for chain_id in valid_ids:
            assert validate_chain_id(chain_id) is True
    
    def test_validate_chain_id_invalid(self):
        """Test chain ID validation with invalid IDs."""
        invalid_ids = [
            "",
            "invalid-format",
            "__chain-12345678",  # Missing repo part
            "repo__chain-",  # Missing hash part
            "repo__chain-123",  # Hash too short
            "repo-chain-12345678",  # Wrong separator
        ]
        
        for chain_id in invalid_ids:
            assert validate_chain_id(chain_id) is False


class TestChainBuilding:
    """Test cases for chain building functionality."""
    
    def test_build_temporal_chains(self):
        """Test building chains based on temporal proximity."""
        # Create instances with different time gaps
        base_time = datetime(2023, 1, 1)
        task_instances = []
        
        for i in range(5):
            # First 3 instances within 1 day, last 2 within 1 day but separate
            if i < 3:
                created_at = base_time + timedelta(hours=i * 6)
            else:
                created_at = base_time + timedelta(days=10 + (i-3))
            
            task_instances.append({
                "repo": "test/repo",
                "pull_number": i + 1,
                "instance_id": f"test__repo-{i+1}",
                "created_at": created_at.isoformat() + "Z"
            })
        
        chains = build_chains_from_repository_data(
            task_instances,
            grouping_strategy="temporal",
            time_window_days=1
        )
        
        # Should create 2 chains: [1,2,3] and [4,5]
        assert len(chains) == 2
        assert len(chains[0]) == 3
        assert len(chains[1]) == 2
    
    def test_build_issue_based_chains(self):
        """Test building chains based on shared issues."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "issue_numbers": ["100"],
                "created_at": "2023-01-01T00:00:00Z"
            },
            {
                "repo": "test/repo",
                "pull_number": 2,
                "instance_id": "test__repo-2",
                "issue_numbers": ["100"],  # Same issue
                "created_at": "2023-01-02T00:00:00Z"
            },
            {
                "repo": "test/repo",
                "pull_number": 3,
                "instance_id": "test__repo-3",
                "issue_numbers": ["200"],  # Different issue
                "created_at": "2023-01-03T00:00:00Z"
            }
        ]
        
        chains = build_chains_from_repository_data(
            task_instances,
            grouping_strategy="issue_based"
        )
        
        # Should create 2 chains: one for issue 100 (PRs 1,2) and one for issue 200 (PR 3)
        assert len(chains) == 2
        
        # Find chains by length
        chain_lengths = [len(chain) for chain in chains]
        assert 2 in chain_lengths  # Chain with 2 instances
        assert 1 in chain_lengths  # Chain with 1 instance


class TestChainFileOperations:
    """Test cases for chain file I/O operations."""
    
    def test_save_and_load_chains(self):
        """Test saving chains to JSONL and loading them back."""
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            }
        ]
        
        original_chains = [Chain(task_instances)]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            temp_file = f.name
        
        try:
            # Save chains
            save_chains_to_jsonl(original_chains, temp_file)
            
            # Load chains back
            loaded_chains = load_chains_from_jsonl(temp_file)
            
            assert len(loaded_chains) == len(original_chains)
            assert loaded_chains[0].chain_id == original_chains[0].chain_id
            assert len(loaded_chains[0]) == len(original_chains[0])
            
        finally:
            os.unlink(temp_file)
    
    def test_convert_single_instances_to_chains(self):
        """Test converting single instances file to chains file."""
        # Create test data
        task_instances = [
            {
                "repo": "test/repo",
                "pull_number": 1,
                "instance_id": "test__repo-1",
                "created_at": "2023-01-01T00:00:00Z"
            },
            {
                "repo": "test/repo",
                "pull_number": 2,
                "instance_id": "test__repo-2",
                "created_at": "2023-01-01T01:00:00Z"  # 1 hour later
            }
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as input_f:
            input_file = input_f.name
            for instance in task_instances:
                input_f.write(json.dumps(instance) + '\n')
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as output_f:
            output_file = output_f.name
        
        try:
            # Convert single instances to chains
            convert_single_instances_to_chains(
                input_file,
                output_file,
                grouping_strategy="temporal"
            )
            
            # Load and verify chains
            chains = load_chains_from_jsonl(output_file)
            
            assert len(chains) == 1  # Should be grouped into one chain
            assert len(chains[0]) == 2  # Should contain both instances
            
        finally:
            os.unlink(input_file)
            os.unlink(output_file)


class TestBackwardCompatibility:
    """Test cases for backward compatibility with existing format."""
    
    def test_chain_with_existing_task_format(self):
        """Test that chains work with existing task instance format."""
        # Use format similar to what's in build_dataset.py
        task_instance = {
            "repo": "pvlib/pvlib-python",
            "pull_number": 1,
            "instance_id": "pvlib__pvlib-python-1",
            "issue_numbers": [],
            "base_commit": "e8dd1d9bdaff50319fde60397d704061290f19de",
            "patch": "diff --git a/README.md b/README.md\n...",
            "test_patch": "",
            "problem_statement": "Update README.md",
            "hints_text": "",
            "created_at": "2015-02-17T01:01:06Z"
        }
        
        # Should work with existing format
        chain = create_single_instance_chain(task_instance)
        
        assert len(chain) == 1
        assert chain[0]["repo"] == "pvlib/pvlib-python"
        assert chain[0]["pull_number"] == 1
        assert "patch" in chain[0]
        assert "problem_statement" in chain[0]
    
    def test_task_instance_format_backward_compatibility(self):
        """Test that task instances work with or without chain fields."""
        # Test without chain fields
        original_instance = {
            "repo": "test/repo",
            "pull_number": 1,
            "instance_id": "test__repo-1",
            "patch": "test patch",
            "problem_statement": "test problem"
        }
        
        # Should work with existing format
        chain = create_single_instance_chain(original_instance)
        assert len(chain) == 1
        
        # Test with chain fields (as created by build_dataset.py)
        instance_with_chain = original_instance.copy()
        instance_with_chain.update({
            "chain_id": None,
            "chain_position": None,
            "is_chain_member": False
        })
        
        # Should also work
        chain2 = create_single_instance_chain(instance_with_chain)
        assert len(chain2) == 1
        assert chain2.metadata["validation_status"] == "passed"
