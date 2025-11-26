# Multi-Turn Evaluation Guide

## 1. The Architecture

### A. Chain Construction (`swebench/collect/chain_construction.py`)

**Goal:** Identify groups of Pull Requests (PRs) that act like a story.

**How it works:**

- **Graph Analysis:**  
  Scans a repository's PR history. If PR `#1` and PR `#2` modify the same files, they are linked.

- **Dependency Graph:**  
  Builds a graph of these links and finds **chains** (sequences of related work).

- **Time Sorting:**  
  Organizes chains chronologically (Turn 0 → Turn 1 → Turn 2).

- **Dynamic Versioning:**  
  Detects the correct software version for each task to ensure the environment is built correctly.

---

### B. Stateful Evaluation Harness (`swebench/harness/run_multi_turn_evaluation.py`)

**Goal:** Test if an agent can solve a chain without breaking previous work.

**How it works:**

- **Persistent Container:**  
  Creates a Docker container that stays alive across the entire chain.

- **Cumulative State:**
  - **Turn 0:** Apply patch → Run tests → Commit state.
  - **Turn 1:** Start from Turn 0's state → Apply next patch → Run tests.
  - **Turn 2+:** Continue from the latest committed state → Apply patch → Run tests.

- **Patching:**  
  Uses a multiple approach (`git apply` → `patch`) to handle context mismatches that occur naturally when stacking edits.

---

### C. Multi-Turn Inference (`swebench/inference/run_multi_turn_api.py`)

**Goal:** Generate model predictions for multi-turn chains using an LLM API.

**How it works:**

- **Conversation History:**  
  Maintains conversation history across turns, simulating a multi-turn coding session.

- **Turn-by-Turn Generation:**
  - **Turn 0:** Present the first issue, model generates a patch.
  - **Turn 1+:** Present the next issue (noting previous patches were applied), model generates the next patch.

- **Patch Extraction:**  
  Automatically extracts unified diff patches from model responses.

---

## 2. How to Run the Pipeline

### Step 1: Collect Data

Fetch the 1000 most recent pull requests from scikit-learn to use as our source data.

```bash
# Create a directory for the test data
mkdir -p swebench/collect/multi_turn_test

# Scrape the PRs
python swebench/collect/print_pulls.py scikit-learn/scikit-learn swebench/collect/multi_turn_test/sklearn-prs.jsonl --max_pulls 1000
```

### Step 2: Build Chains

Convert raw PRs into multi-turn chains:

```bash
# Run the construction runner script
python swebench/collect/run_chain_construction.py
```
Output: This will create `swebench/collect/multi_turn_test/sklearn-chains.jsonl`

### Step 3: Generate Predictions (Optional)

Generate model predictions using an API:

```bash
# Using OpenAI (set OPENAI_API_KEY env var)
python -m swebench.inference.run_multi_turn_api \
    --chains_path swebench/collect/multi_turn_test/sklearn-chains.jsonl \
    --model_name gpt-4o \
    --output_file predictions/sklearn-chains-gpt4o.jsonl

# Using Anthropic (set ANTHROPIC_API_KEY env var)
python -m swebench.inference.run_multi_turn_api \
    --chains_path swebench/collect/multi_turn_test/sklearn-chains.jsonl \
    --model_name claude-3-5-sonnet-20241022 \
    --output_file predictions/sklearn-chains-claude.jsonl
```

### Step 4: Run Evaluation

Run the stateful evaluation harness:

```bash
# Using gold patches (from the chains themselves)
python -m swebench.harness.run_multi_turn_evaluation \
    --chains_path swebench/collect/multi_turn_test/sklearn-chains.jsonl \
    --predictions_path gold \
    --run_id sklearn_gold_run

# Using model predictions
python -m swebench.harness.run_multi_turn_evaluation \
    --chains_path swebench/collect/multi_turn_test/sklearn-chains.jsonl \
    --predictions_path predictions/sklearn-chains-gpt4o.jsonl \
    --run_id sklearn_gpt4o_run
```

---

## 3. Prediction Format

### Multi-Turn Chain Predictions

Predictions for multi-turn evaluation should be in JSONL format with each line containing:

```json
{
    "chain_id": "repo__chain-abc12345",
    "model_name_or_path": "gpt-4o",
    "turn_predictions": [
        {"instance_id": "repo__repo-1234", "model_patch": "diff content..."},
        {"instance_id": "repo__repo-1235", "model_patch": "diff content..."},
        {"instance_id": "repo__repo-1236", "model_patch": "diff content..."}
    ]
}
```

Each `turn_predictions` list should match the order of `task_instances` in the chain.

---

## 4. Output and Metrics

### Chain Metrics

After evaluation, each chain produces a `chain_metrics.json` file with:

```json
{
    "total_turns": 3,
    "resolved_turns": 2,
    "success_rate": 0.67,
    "trajectory_streak": 2,
    "full_chain_success": false,
    "turn_results": [true, true, false]
}
```

**Metrics explained:**

- **total_turns**: Number of turns in the chain
- **resolved_turns**: Number of turns that passed all tests
- **success_rate**: Fraction of turns that passed (resolved_turns / total_turns)
- **trajectory_streak**: Number of consecutive passes from the start (important for sequential dependencies)
- **full_chain_success**: Whether all turns passed
- **turn_results**: List of boolean pass/fail status for each turn in order (e.g., `[true, true, false]` means turns 0 and 1 passed, turn 2 failed)

### Summary Report

A `summary_metrics.json` file is generated with aggregate statistics across all chains:

```json
{
    "run_id": "sklearn_gpt4o_run",
    "predictions_path": "predictions/sklearn-chains-gpt4o.jsonl",
    "total_chains": 10,
    "aggregate": {
        "total_turns": 35,
        "total_resolved": 25,
        "overall_success_rate": 0.7143,
        "full_chain_success_count": 4,
        "full_chain_success_rate": 0.4
    }
}
```

---

## 5. Command Line Options

### Evaluation (`run_multi_turn_evaluation.py`)

| Argument | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `--chains_path` | str | Yes | - | Path to chains JSONL file |
| `-p, --predictions_path` | str | Yes | - | Path to predictions file or "gold" for gold patches |
| `-id, --run_id` | str | Yes | - | Unique identifier for this run |
| `--max_workers` | int | No | 4 | Max parallel workers for image building |
| `-t, --timeout` | int | No | 1800 | Timeout per turn (seconds) |

### Inference (`run_multi_turn_api.py`)

| Argument | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `--chains_path` | str | Yes | - | Path to chains JSONL file |
| `--model_name` | str | Yes | - | Model name (e.g., gpt-4o, claude-3-5-sonnet-20241022) |
| `--output_file` | str | Yes | - | Output predictions file path |
| `--temperature` | float | No | 0.2 | Sampling temperature |
| `--top_p` | float | No | 0.95 | Top-p sampling parameter |
| `--max_tokens` | int | No | 4096 | Max tokens per response |
| `--max_cost` | float | No | None | Maximum total cost (stops when reached) |
| `--chain_ids` | str[] | No | None | Specific chain IDs to process |
