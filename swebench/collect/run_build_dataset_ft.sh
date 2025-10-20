#!/bin/bash

uv run python build_dataset_ft.py \
    --instances_path "<path to folder containing task instance (raw) files>" \
    --output_path "<path to folder to save finetuning dataset to>" \
    --eval_path "<path to folder containing all evaluation task instances>"