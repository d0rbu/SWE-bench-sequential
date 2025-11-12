#!/usr/bin/env bash

# Build chains from single task instances using DAG-based dependency analysis
#
# This script converts task instances (from run_get_tasks_pipeline.sh) into chains
# of related PRs based on git blame analysis and issue dependencies.
#
# Prerequisites:
#   1. Run run_get_tasks_pipeline.sh to generate task instances
#   2. Clone the repository you want to analyze (if not already cloned)
#
# Usage:
#   ./run_build_chains_pipeline.sh <repo_owner/repo_name>
#
# Example:
#   ./run_build_chains_pipeline.sh scikit-learn/scikit-learn
#
# The script will:
#   - Clone the repository to ./repos/ if it doesn't exist
#   - Look for task instances in ./tasks/
#   - Generate chains in ./chains/

set -e  # Exit on error

# Get repo from command line or use default
REPO="${1:-scikit-learn/scikit-learn}"
REPO_NAME="${REPO##*/}"
REPO_OWNER="${REPO%/*}"

# Set up directories
REPOS_DIR="repos"
TASKS_DIR="tasks"
CHAINS_DIR="chains"

mkdir -p "$REPOS_DIR" "$CHAINS_DIR"

# Clone repository if it doesn't exist
REPO_PATH="$REPOS_DIR/$REPO_NAME"
if [ ! -d "$REPO_PATH" ]; then
    echo "📦 Cloning $REPO to $REPO_PATH..."
    git clone "https://github.com/$REPO.git" "$REPO_PATH"
    echo "✅ Repository cloned successfully"
else
    echo "📁 Repository already exists at $REPO_PATH"
fi

# Set file paths
INPUT_FILE="$TASKS_DIR/$REPO_NAME-task-instances.jsonl"
OUTPUT_FILE="$CHAINS_DIR/$REPO_NAME-chains.jsonl"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "❌ Error: Task instances file not found: $INPUT_FILE"
    echo "   Please run run_get_tasks_pipeline.sh first to generate task instances."
    exit 1
fi

echo "🔗 Building chains for $REPO..."
echo "   Input: $INPUT_FILE"
echo "   Output: $OUTPUT_FILE"
echo "   Repo: $REPO_PATH"

# Run the chain building pipeline
uv run python build_chains_pipeline.py \
    --input_file "$INPUT_FILE" \
    --output_file "$OUTPUT_FILE" \
    --repo_path "$REPO_PATH" \
    --num_chains 10 \
    --min_chain_length 2 \
    --max_chain_length 5 \
    --blame_threshold 0.05 \
    --time_window_months 6

echo "✅ Done! Chains saved to $OUTPUT_FILE"
