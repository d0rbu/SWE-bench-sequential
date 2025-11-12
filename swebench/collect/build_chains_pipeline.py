#!/usr/bin/env python3

"""Script to build chains from single task instances using DAG-based dependency analysis"""

import argparse
import json
import os

from swebench.collect.chain_construction import (
    convert_single_instances_to_chains,
)


def main(
    input_file: str,
    output_file: str,
    repo_path: str,
    num_chains: int = 10,
    min_chain_length: int = 2,
    max_chain_length: int = 5,
    blame_threshold: float = 0.1,
    time_window_months: int = 6,
):
    """
    Build chains from single task instances using DAG-based dependency analysis.
    
    This script converts a JSONL file containing single task instances into chains
    of related task instances based on git blame analysis and issue dependencies.
    
    Args:
        input_file: Path to input JSONL file containing single task instances
        output_file: Path to output JSONL file for chains
        repo_path: Path to git repository for blame analysis (required)
        num_chains: Number of chains to build (default: 10)
        min_chain_length: Minimum length of chains to include (default: 2)
        max_chain_length: Maximum length of chains to create (default: 5)
        blame_threshold: Minimum blame percentage to consider dependency (default: 0.1)
        time_window_months: Time window in months for considering dependencies (default: 6)
    """
    # Validate inputs
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if not os.path.exists(repo_path):
        raise FileNotFoundError(f"Repository path not found: {repo_path}")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    print(f"Building chains from task instances...")
    print(f"  Input file: {input_file}")
    print(f"  Output file: {output_file}")
    print(f"  Repository: {repo_path}")
    print(f"  Configuration:")
    print(f"    - Number of chains: {num_chains}")
    print(f"    - Chain length: {min_chain_length}-{max_chain_length}")
    print(f"    - Blame threshold: {blame_threshold}")
    print(f"    - Time window: {time_window_months} months")
    
    # Convert single instances to chains
    convert_single_instances_to_chains(
        input_file=input_file,
        output_file=output_file,
        repo_path=repo_path,
        num_chains=num_chains,
        min_chain_length=min_chain_length,
        max_chain_length=max_chain_length,
        blame_threshold=blame_threshold,
        time_window_months=time_window_months,
    )
    
    # Count chains in output
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            num_chains_created = sum(1 for _ in f)
        print(f"✅ Successfully created {num_chains_created} chains")
        print(f"   Saved to: {output_file}")
    else:
        print("⚠️ Warning: Output file was not created")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input_file",
        type=str,
        required=True,
        help="Path to input JSONL file containing single task instances",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        required=True,
        help="Path to output JSONL file for chains",
    )
    parser.add_argument(
        "--repo_path",
        type=str,
        required=True,
        help="Path to git repository for blame analysis",
    )
    parser.add_argument(
        "--num_chains",
        type=int,
        default=10,
        help="Number of chains to build (default: 10)",
    )
    parser.add_argument(
        "--min_chain_length",
        type=int,
        default=2,
        help="Minimum length of chains to include (default: 2)",
    )
    parser.add_argument(
        "--max_chain_length",
        type=int,
        default=5,
        help="Maximum length of chains to create (default: 5)",
    )
    parser.add_argument(
        "--blame_threshold",
        type=float,
        default=0.1,
        help="Minimum blame percentage to consider dependency (default: 0.1)",
    )
    parser.add_argument(
        "--time_window_months",
        type=int,
        default=6,
        help="Time window in months for considering dependencies (default: 6)",
    )
    args = parser.parse_args()
    main(**vars(args))

