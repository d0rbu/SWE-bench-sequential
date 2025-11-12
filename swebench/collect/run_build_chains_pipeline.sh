#!/usr/bin/env bash

# Build chains from single task instances using DAG-based dependency analysis
#
# This script converts task instances (from run_get_tasks_pipeline.sh) into chains
# of related PRs based on git blame analysis and issue dependencies.
#
# Usage:
#   1. First run run_get_tasks_pipeline.sh to generate task instances
#   2. Then run this script to build chains from those instances
#
# Example:
#   ./run_build_chains_pipeline.sh

uv run python build_chains_pipeline.py \
    --input_file 'tasks/scikit-learn-task-instances.jsonl' \
    --output_file 'chains/scikit-learn-chains.jsonl' \
    --repo_path '/path/to/scikit-learn' \
    --num_chains 10 \
    --min_chain_length 2 \
    --max_chain_length 5

