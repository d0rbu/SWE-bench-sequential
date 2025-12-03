#!/usr/bin/env python3

"""Script to build chains from single task instances using DAG-based dependency analysis"""

import argparse
import json
import logging
import os

from swebench.collect.chain_construction import (
    convert_single_instances_to_chains,
)


def main(
    input_file: str,
    output_file: str,
    num_chains: int = 10,
    min_chain_length: int = 2,
    max_chain_length: int = 5,
    file_overlap_threshold: float = 0.0,
    time_window_months: int = 6,
    compute_fail_to_pass: bool = True,
    fail_to_pass_timeout: int = 1800,
    max_workers: int = 10,
    cache_dir: str | None = None,
    use_cache: bool = True,
    log_level: str = "INFO",
):
    """
    Build chains from single task instances using DAG-based dependency analysis.
    
    This script converts a JSONL file containing single task instances into chains
    of related task instances based on file overlap and issue dependencies.
    
    Args:
        input_file: Path to input JSONL file containing single task instances
        output_file: Path to output JSONL file for chains
        num_chains: Number of chains to build (default: 10)
        min_chain_length: Minimum length of chains to include (default: 2)
        max_chain_length: Maximum length of chains to create (default: 5)
        file_overlap_threshold: Minimum file overlap weight to consider dependency (default: 0.0 = any overlap)
        time_window_months: Time window in months for considering dependencies (default: 6)
        compute_fail_to_pass: If True, compute FAIL_TO_PASS by running tests (default: True)
        fail_to_pass_timeout: Timeout for FAIL_TO_PASS computation per instance in seconds (default: 1800)
        max_workers: Number of parallel workers for FAIL_TO_PASS computation (default: 10)
        cache_dir: Directory to store FAIL_TO_PASS cache (default: .swebench_cache)
        use_cache: Whether to use cached FAIL_TO_PASS results (default: True)
        log_level: Logging level (default: INFO)
    """
    # Configure logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    # Set up logging for all relevant modules
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True  # Override any existing configuration
    )
    
    # Set specific loggers
    logging.getLogger('swebench.collect.dag_dependency_analysis').setLevel(numeric_level)
    logging.getLogger('swebench.collect.chain_construction').setLevel(numeric_level)
    
    # Validate inputs
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    print(f"Building chains from task instances...")
    print(f"  Input file: {input_file}")
    print(f"  Output file: {output_file}")
    print(f"  Configuration:")
    print(f"    - Number of chains: {num_chains}")
    print(f"    - Chain length: {min_chain_length}-{max_chain_length}")
    print(f"    - File overlap threshold: {file_overlap_threshold}")
    print(f"    - Time window: {time_window_months} months")
    print(f"    - Compute FAIL_TO_PASS: {compute_fail_to_pass}")
    if compute_fail_to_pass:
        print(f"    - Max workers: {max_workers}")
        print(f"    - Cache directory: {cache_dir or '.swebench_cache'}")
        print(f"    - Use cache: {use_cache}")
    
    # Convert single instances to chains
    convert_single_instances_to_chains(
        input_file=input_file,
        output_file=output_file,
        num_chains=num_chains,
        min_chain_length=min_chain_length,
        max_chain_length=max_chain_length,
        file_overlap_threshold=file_overlap_threshold,
        time_window_months=time_window_months,
        compute_fail_to_pass=compute_fail_to_pass,
        fail_to_pass_timeout=fail_to_pass_timeout,
        max_workers=max_workers,
        cache_dir=cache_dir,
        use_cache=use_cache,
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
        "--file_overlap_threshold",
        type=float,
        default=0.0,
        help="Minimum file overlap weight to consider dependency (default: 0.0 = any overlap)",
    )
    parser.add_argument(
        "--time_window_months",
        type=int,
        default=6,
        help="Time window in months for considering dependencies (default: 6)",
    )
    parser.add_argument(
        "--compute_fail_to_pass",
        type=lambda x: str(x).lower() in ["true", "1", "yes"],
        default=True,
        help="Whether to compute FAIL_TO_PASS by running tests (default: True)",
    )
    parser.add_argument(
        "--fail_to_pass_timeout",
        type=int,
        default=1800,
        help="Timeout for FAIL_TO_PASS computation per instance in seconds (default: 1800)",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=10,
        help="Number of parallel workers for FAIL_TO_PASS computation (default: 10)",
    )
    parser.add_argument(
        "--cache_dir",
        type=str,
        default=None,
        help="Directory to store FAIL_TO_PASS cache (default: .swebench_cache)",
    )
    parser.add_argument(
        "--use_cache",
        action="store_true",
        default=True,
        help="Use cached FAIL_TO_PASS results (default: True)",
    )
    parser.add_argument(
        "--no_cache",
        action="store_false",
        dest="use_cache",
        help="Don't use cached FAIL_TO_PASS results, recompute from scratch",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()
    main(**vars(args))
