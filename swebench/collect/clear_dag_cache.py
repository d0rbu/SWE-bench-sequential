#!/usr/bin/env python3

"""Script to clear cached DAG computations."""

import argparse
import logging

from swebench.collect.chain_construction import clear_dag_cache

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main(cache_dir: str = None):
    """
    Clear all cached DAG files.
    
    Args:
        cache_dir: Directory containing cache files (default: .swebench_cache)
    """
    print(f"Clearing DAG cache from: {cache_dir or '.swebench_cache'}")
    clear_dag_cache(cache_dir)
    print("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache_dir",
        type=str,
        default=None,
        help="Directory containing cache files (default: .swebench_cache)",
    )
    args = parser.parse_args()
    main(**vars(args))
