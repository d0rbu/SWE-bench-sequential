# DAG Computation Caching

The chain construction pipeline now supports caching of the expensive DAG (Directed Acyclic Graph) computation to disk. This allows you to resume work without having to wait for the entire computation to complete every time.

## How It Works

When building chains from task instances, the most time-consuming step is computing the dependency DAG. This involves:
- Parsing patches to extract modified files
- Computing file overlap between PRs
- Analyzing temporal proximity and issue relationships

The cache stores the computed DAG to disk so that subsequent runs with the same input data and parameters can skip this expensive computation.

## Cache Key

The cache key is computed from:
- All task instance IDs (sorted)
- `time_window_months` parameter
- `file_overlap_threshold` parameter

If any of these change, a new DAG will be computed. The chain sampling parameters (`num_chains`, `min_chain_length`, `max_chain_length`, `sampler`, `seed`) do NOT affect the cache key, since they only affect sampling from the DAG, not the DAG itself.

## Usage

### Basic Usage (with caching enabled by default)

```bash
python swebench/collect/build_chains_pipeline.py \
    --input_file data/instances.jsonl \
    --output_file data/chains.jsonl \
    --num_chains 10
```

The first run will compute the DAG and cache it. Subsequent runs with the same input file and DAG parameters will load from cache.

### Disable Caching

To force recomputation from scratch:

```bash
python swebench/collect/build_chains_pipeline.py \
    --input_file data/instances.jsonl \
    --output_file data/chains.jsonl \
    --no_cache
```

### Custom Cache Directory

By default, caches are stored in `.swebench_cache/`. To use a different directory:

```bash
python swebench/collect/build_chains_pipeline.py \
    --input_file data/instances.jsonl \
    --output_file data/chains.jsonl \
    --cache_dir /path/to/cache
```

### Clear Cache

To clear all cached DAG files:

```bash
python swebench/collect/clear_dag_cache.py
```

Or with a custom cache directory:

```bash
python swebench/collect/clear_dag_cache.py --cache_dir /path/to/cache
```

## Cache Files

Cache files are stored as:
- Location: `.swebench_cache/` (by default)
- Naming: `dag_cache_<hash>.pkl`
- Format: Python pickle files containing the DependencyDAG object

## When Cache is Invalidated

The cache is automatically invalidated (a new cache entry is created) when:
- The set of task instances changes (different input file or modified instances)
- The `time_window_months` parameter changes
- The `file_overlap_threshold` parameter changes

## Python API

You can also use the caching feature programmatically:

```python
from swebench.collect.chain_construction import (
    build_chains_from_repository_data,
    clear_dag_cache,
    load_task_instances,
)

# Load instances
task_instances = load_task_instances("data/instances.jsonl")

# Build chains with caching (default)
chains = build_chains_from_repository_data(
    task_instances,
    num_chains=10,
    use_cache=True,  # default
    cache_dir=".swebench_cache"  # default
)

# Build chains without caching
chains = build_chains_from_repository_data(
    task_instances,
    num_chains=10,
    use_cache=False
)

# Clear all cached DAGs
clear_dag_cache()
```

## Performance Impact

For large datasets with hundreds of task instances:
- First run: Can take several minutes to hours (depending on dataset size)
- Cached runs: Typically completes in seconds

The cache only stores the DAG structure. Chain sampling is always performed fresh based on your specified parameters.
