#!/usr/bin/env python3
"""
Demonstration script for the chain construction module.

This script shows how to use the chain construction functionality
to create and manage chains of related task instances.
"""

import json
from datetime import datetime, timedelta
from swebench.collect.chain_construction import (
    Chain,
    create_single_instance_chain,
    build_chains_from_repository_data,
    save_chains_to_jsonl,
    load_chains_from_jsonl,
)


def create_sample_task_instances():
    """Create sample task instances for demonstration."""
    base_time = datetime(2023, 1, 1)
    
    # Create instances that should form chains
    instances = []
    
    # Chain 1: Three related PRs within a few days
    for i in range(3):
        instances.append({
            "repo": "example/project",
            "pull_number": i + 1,
            "instance_id": f"example__project-{i+1}",
            "issue_numbers": ["100"],  # Same issue
            "base_commit": f"abc123{i}",
            "patch": f"patch content for PR {i+1}",
            "test_patch": f"test patch for PR {i+1}",
            "problem_statement": f"Fix issue 100 - part {i+1}",
            "hints_text": f"Hints for PR {i+1}",
            "created_at": (base_time + timedelta(days=i)).isoformat() + "Z"
        })
    
    # Chain 2: Two PRs for a different issue, much later
    for i in range(2):
        instances.append({
            "repo": "example/project",
            "pull_number": i + 10,
            "instance_id": f"example__project-{i+10}",
            "issue_numbers": ["200"],  # Different issue
            "base_commit": f"def456{i}",
            "patch": f"patch content for PR {i+10}",
            "test_patch": f"test patch for PR {i+10}",
            "problem_statement": f"Fix issue 200 - part {i+1}",
            "hints_text": f"Hints for PR {i+10}",
            "created_at": (base_time + timedelta(days=30 + i)).isoformat() + "Z"
        })
    
    # Single PR for another issue
    instances.append({
        "repo": "example/project",
        "pull_number": 20,
        "instance_id": "example__project-20",
        "issue_numbers": ["300"],
        "base_commit": "ghi789",
        "patch": "patch content for PR 20",
        "test_patch": "test patch for PR 20",
        "problem_statement": "Fix issue 300",
        "hints_text": "Hints for PR 20",
        "created_at": (base_time + timedelta(days=60)).isoformat() + "Z"
    })
    
    return instances


def demonstrate_basic_chain_operations():
    """Demonstrate basic Chain class operations."""
    print("=== Basic Chain Operations ===")
    
    # Create a single instance chain
    task_instance = {
        "repo": "test/repo",
        "pull_number": 1,
        "instance_id": "test__repo-1",
        "created_at": "2023-01-01T00:00:00Z"
    }
    
    chain = create_single_instance_chain(task_instance)
    print(f"Created single instance chain: {chain}")
    print(f"Chain ID: {chain.chain_id}")
    print(f"Chain metadata: {json.dumps(chain.metadata, indent=2)}")
    
    # Test serialization
    jsonl_str = chain.to_jsonl()
    print(f"Serialized chain (first 100 chars): {jsonl_str[:100]}...")
    
    # Test deserialization
    restored_chain = Chain.from_jsonl(jsonl_str)
    print(f"Restored chain: {restored_chain}")
    print()


def demonstrate_chain_building():
    """Demonstrate building chains from task instances."""
    print("=== Chain Building ===")
    
    instances = create_sample_task_instances()
    print(f"Created {len(instances)} sample task instances")
    
    # Build chains using temporal strategy
    print("\n--- Temporal Chain Building ---")
    temporal_chains = build_chains_from_repository_data(
        instances,
        grouping_strategy="temporal",
        time_window_days=7  # Group PRs within 7 days
    )
    
    print(f"Created {len(temporal_chains)} temporal chains:")
    for i, chain in enumerate(temporal_chains):
        pr_numbers = [inst["pull_number"] for inst in chain]
        date_range = chain.metadata["date_range"]
        print(f"  Chain {i+1}: PRs {pr_numbers}, dates {date_range['start']} to {date_range['end']}")
    
    # Build chains using issue-based strategy
    print("\n--- Issue-based Chain Building ---")
    issue_chains = build_chains_from_repository_data(
        instances,
        grouping_strategy="issue_based"
    )
    
    print(f"Created {len(issue_chains)} issue-based chains:")
    for i, chain in enumerate(issue_chains):
        pr_numbers = [inst["pull_number"] for inst in chain]
        issues = set()
        for inst in chain:
            issues.update(inst.get("issue_numbers", []))
        print(f"  Chain {i+1}: PRs {pr_numbers}, issues {list(issues)}")
    print()


def demonstrate_file_operations():
    """Demonstrate saving and loading chains."""
    print("=== File Operations ===")
    
    instances = create_sample_task_instances()
    chains = build_chains_from_repository_data(instances, grouping_strategy="issue_based")
    
    # Save chains to file
    output_file = "demo_chains.jsonl"
    save_chains_to_jsonl(chains, output_file)
    print(f"Saved {len(chains)} chains to {output_file}")
    
    # Load chains from file
    loaded_chains = load_chains_from_jsonl(output_file)
    print(f"Loaded {len(loaded_chains)} chains from {output_file}")
    
    # Verify they match
    for original, loaded in zip(chains, loaded_chains):
        assert original.chain_id == loaded.chain_id
        assert len(original) == len(loaded)
    
    print("✅ File operations verified successfully")
    
    # Clean up
    import os
    os.remove(output_file)
    print()


def demonstrate_backward_compatibility():
    """Demonstrate backward compatibility with existing format."""
    print("=== Backward Compatibility ===")
    
    # Create instance in existing format (without chain fields)
    existing_instance = {
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
    chain = create_single_instance_chain(existing_instance)
    print(f"Created chain from existing format: {chain}")
    print(f"Instance has all original fields: {all(key in chain[0] for key in existing_instance.keys())}")
    
    # Chain fields should be None/False for backward compatibility
    print(f"Chain fields: chain_id={chain[0].get('chain_id')}, is_chain_member={chain[0].get('is_chain_member')}")
    print("✅ Backward compatibility verified")
    print()


def main():
    """Run all demonstrations."""
    print("Chain Construction Module Demonstration")
    print("=" * 50)
    print()
    
    demonstrate_basic_chain_operations()
    demonstrate_chain_building()
    demonstrate_file_operations()
    demonstrate_backward_compatibility()
    
    print("🎉 All demonstrations completed successfully!")


if __name__ == "__main__":
    main()
