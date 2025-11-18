# 1. The Architecture

## A. Chain Construction (`swebench/collect/chain_construction.py`)

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

## B. Stateful Evaluation Harness (`swebench/harness/run_multi_turn_evaluation.py`)

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

# 2. How to Run the Pipeline

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
Output: This will create swebench/collect/multi_turn_test/sklearn-chains.jsonl

### Step 3: Run Evaluation

Run the stateful evaluation harness:

```bash
python -m swebench.harness.run_multi_turn_evaluation \
    --chains_path swebench/collect/multi_turn_test/sklearn-chains.jsonl \
    --run_id sklearn_pilot_run_001
```