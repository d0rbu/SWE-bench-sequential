#!/usr/bin/env bash

# Complete pipeline: Collect PRs, generate task instances, and build chains
#
# This script is a superset of run_get_tasks_pipeline.sh that also builds chains.
# It performs the full workflow:
#   1. Clone repository (if needed)
#   2. Collect PR data from GitHub
#   3. Convert PRs to task instances
#   4. Build chains using DAG-based dependency analysis
#
# Usage:
#   ./run_build_chains_pipeline.sh <repo_owner/repo_name> [github_token]
#
# Example:
#   ./run_build_chains_pipeline.sh scikit-learn/scikit-learn
#   ./run_build_chains_pipeline.sh pallets/flask $GITHUB_TOKEN
#
# If you'd like to parallelize, create a .env file in this directory with:
#   GITHUB_TOKENS=token1,token2,token3...

set -e  # Exit on error

# Load .env file if it exists (for GITHUB_TOKENS support)
if [ -f .env ]; then
    echo "📋 Loading environment variables from .env file..."
    set -a  # automatically export all variables
    source .env
    set +a
fi

# Get repo from command line or use default
REPO="${1:-scikit-learn/scikit-learn}"
REPO_NAME="${REPO##*/}"
REPO_OWNER="${REPO%/*}"

# Get GitHub token from command line, GITHUB_TOKEN env var, or GITHUB_TOKENS (first token)
if [ -n "$2" ]; then
    GITHUB_TOKEN="$2"
elif [ -n "$GITHUB_TOKEN" ]; then
    GITHUB_TOKEN="$GITHUB_TOKEN"
elif [ -n "$GITHUB_TOKENS" ]; then
    # Extract first token from comma-separated list
    GITHUB_TOKEN="${GITHUB_TOKENS%%,*}"
    echo "📋 Using first token from GITHUB_TOKENS for single-repo pipeline"
else
    GITHUB_TOKEN=""
fi

# Set up directories
REPOS_DIR="repos"
PRS_DIR="prs"
TASKS_DIR="tasks"
CHAINS_DIR="chains"

mkdir -p "$REPOS_DIR" "$PRS_DIR" "$TASKS_DIR" "$CHAINS_DIR"

echo "════════════════════════════════════════════════════════════"
echo "  Complete Pipeline for $REPO"
echo "════════════════════════════════════════════════════════════"
echo ""

# Step 1: Clone repository if it doesn't exist
echo "📦 Step 1/4: Repository Setup"
REPO_PATH="$REPOS_DIR/$REPO_NAME"
if [ ! -d "$REPO_PATH" ]; then
    echo "   Cloning $REPO to $REPO_PATH..."
    git clone "https://github.com/$REPO.git" "$REPO_PATH"
    echo "   ✅ Repository cloned successfully"
else
    echo "   ✅ Repository already exists at $REPO_PATH"
fi
echo ""

# Step 2: Collect PR data from GitHub
echo "🔍 Step 2/4: Collecting PR Data from GitHub"
PRS_FILE="$PRS_DIR/$REPO_NAME-prs.jsonl"
if [ -n "$GITHUB_TOKEN" ]; then
    echo "   Using provided GitHub token"
    uv run python print_pulls.py "$REPO" "$PRS_FILE" --token "$GITHUB_TOKEN"
else
    echo "   No GitHub token provided, using anonymous access (rate limited)"
    uv run python print_pulls.py "$REPO" "$PRS_FILE"
fi
echo "   ✅ PR data saved to $PRS_FILE"
echo ""

# Step 3: Convert PRs to task instances
echo "🔧 Step 3/4: Converting PRs to Task Instances"
TASKS_FILE="$TASKS_DIR/$REPO_NAME-task-instances.jsonl"
if [ -n "$GITHUB_TOKEN" ]; then
    uv run python build_dataset.py "$PRS_FILE" "$TASKS_FILE" --token "$GITHUB_TOKEN"
else
    uv run python build_dataset.py "$PRS_FILE" "$TASKS_FILE"
fi
echo "   ✅ Task instances saved to $TASKS_FILE"
echo ""

# Step 4: Build chains from task instances
echo "🔗 Step 4/4: Building Chains with DAG Analysis"
CHAINS_FILE="$CHAINS_DIR/$REPO_NAME-chains.jsonl"
echo "   Configuration:"
echo "     - Repository: $REPO_PATH"
echo "     - Num chains: 10"
echo "     - Chain length: 2-5"
echo "     - Blame threshold: 0.05"
echo "     - Time window: 6 months"

uv run python build_chains_pipeline.py \
    --input_file "$TASKS_FILE" \
    --output_file "$CHAINS_FILE" \
    --repo_path "$REPO_PATH" \
    --num_chains 10 \
    --min_chain_length 2 \
    --max_chain_length 5 \
    --blame_threshold 0.05 \
    --time_window_months 6

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  ✅ Pipeline Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Generated files:"
echo "  📄 PRs:           $PRS_FILE"
echo "  📝 Task instances: $TASKS_FILE"
echo "  🔗 Chains:        $CHAINS_FILE"
echo ""
