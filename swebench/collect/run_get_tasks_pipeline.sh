#!/usr/bin/env bash

# If you'd like to parallelize, do the following:
# * Create a .env file in this folder
# * Declare GITHUB_TOKENS=token1,token2,token3...

uv run python get_tasks_pipeline.py \
    --repos 'scikit-learn/scikit-learn', 'pallets/flask' \
    --path_prs 'prs' \
    --path_tasks 'tasks'