#!/bin/bash

uv run python build_dataset_ft.py \
    --instances_path "tasks" \
    --output_path "ft_tasks" \
    --eval_path "eval_tasks"