#!/usr/bin/env bash

# Complete pipeline: Collect PRs, generate task instances, add versioning, and build chains
#
# This script is a superset of run_get_tasks_pipeline.sh that also builds chains.
# It performs the full workflow:
#   1. Clone repository (if needed)
#   2. Collect PR data from GitHub
#   3. Convert PRs to task instances
#   4. Add version information to task instances
#   5. Build chains using DAG-based dependency analysis
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
TASKS_VERSIONED_DIR="tasks-versioned"
CHAINS_DIR="chains"

mkdir -p "$REPOS_DIR" "$PRS_DIR" "$TASKS_DIR" "$TASKS_VERSIONED_DIR" "$CHAINS_DIR"

echo "════════════════════════════════════════════════════════════"
echo "  Complete Pipeline for $REPO"
echo "════════════════════════════════════════════════════════════"
echo ""

# Step 1: Clone repository if it doesn't exist
echo "📦 Step 1/5: Repository Setup"
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
echo "🔍 Step 2/5: Collecting PR Data from GitHub"
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
echo "🔧 Step 3/5: Converting PRs to Task Instances"
TASKS_FILE="$TASKS_DIR/$REPO_NAME-task-instances.jsonl"
if [ ! -f "$TASKS_FILE" ]; then
    echo "   Building task instances..."
    if [ -n "$GITHUB_TOKEN" ]; then
        uv run python build_dataset.py "$PRS_FILE" "$TASKS_FILE" --token "$GITHUB_TOKEN"
    else
        uv run python build_dataset.py "$PRS_FILE" "$TASKS_FILE"
    fi
    echo "   ✅ Task instances saved to $TASKS_FILE"
else
    echo "   ✅ Task instances already exist at $TASKS_FILE"
fi
echo ""

# Step 4: Add version information to task instances
echo "📌 Step 4/5: Adding Version Information"
# Output filename follows get_versions.py convention: {input_basename}_versions.json
TASKS_VERSIONED_FILE="$TASKS_VERSIONED_DIR/${REPO_NAME}-task-instances_versions.json"
if [ ! -f "$TASKS_VERSIONED_FILE" ]; then
    echo "   Retrieving version information from GitHub..."
    uv run python ../versioning/get_versions.py \
        --instances_path "$TASKS_FILE" \
        --retrieval_method github \
        --output_dir "$TASKS_VERSIONED_DIR" \
        --num_workers 4
    echo "   ✅ Versioned task instances saved to $TASKS_VERSIONED_FILE"
else
    echo "   ✅ Versioned task instances already exist at $TASKS_VERSIONED_FILE"
fi
echo ""

# Step 5: Build chains from versioned task instances
echo "🔗 Step 5/5: Building Chains with DAG Analysis"
CHAINS_FILE="$CHAINS_DIR/$REPO_NAME-chains.jsonl"
echo "   Configuration:"
echo "     - Repository: $REPO_PATH"
echo "     - Input: $TASKS_VERSIONED_FILE (with version info)"
echo "     - Num chains: 10"
echo "     - Chain length: 2-5"
echo "     - Blame threshold: 0.05"
echo "     - Time window: 6 months"

uv run python build_chains_pipeline.py \
    --input_file "$TASKS_VERSIONED_FILE" \
    --output_file "$CHAINS_FILE" \
    --num_chains 10 \
    --min_chain_length 2 \
    --max_chain_length 5 \
    --file_overlap_threshold 0.2 \
    --time_window_months 12 \
    --log_level DEBUG

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  ✅ Pipeline Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Generated files:"
echo "  📄 PRs:              $PRS_FILE"
echo "  📝 Task instances:   $TASKS_FILE"
echo "  📌 Versioned tasks:  $TASKS_VERSIONED_FILE"
echo "  🔗 Chains:           $CHAINS_FILE"
echo ""
