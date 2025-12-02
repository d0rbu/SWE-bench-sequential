# Usage Example: Resumable Chain Construction

This example demonstrates how the new caching feature works in practice.

## Scenario

You have a dataset of 500 task instances and want to experiment with different chain configurations.

## First Run (Cold Cache)

```bash
$ python swebench/collect/build_chains_pipeline.py \
    --input_file data/my_instances.jsonl \
    --output_file data/chains_v1.jsonl \
    --num_chains 10 \
    --min_chain_length 2 \
    --max_chain_length 5

Building chains from task instances...
  Input file: data/my_instances.jsonl
  Output file: data/chains_v1.jsonl
  Configuration:
    - Number of chains: 10
    - Chain length: 2-5
    - File overlap threshold: 0.0
    - Time window: 6 months
    - Cache directory: .swebench_cache
    - Use cache: True

INFO - Building dependency DAG from 500 task instances
INFO - Analyzing PR dependencies: 100%|████████████| 500/500
INFO - DAG built with 500 nodes and 1243 edges
INFO - Saved DAG to cache: .swebench_cache/dag_cache_a3f5b8c9d2e1f4a7.pkl
INFO - Sampled 10 diverse chains from DAG

✅ Successfully created 10 chains
   Saved to: data/chains_v1.jsonl

Time elapsed: 5 minutes 23 seconds
```

The DAG computation took ~5 minutes and was saved to cache.

## Second Run (Warm Cache, Different Chain Parameters)

Now you want to try with 20 chains instead of 10:

```bash
$ python swebench/collect/build_chains_pipeline.py \
    --input_file data/my_instances.jsonl \
    --output_file data/chains_v2.jsonl \
    --num_chains 20 \
    --min_chain_length 2 \
    --max_chain_length 5

Building chains from task instances...
  Input file: data/my_instances.jsonl
  Output file: data/chains_v2.jsonl
  Configuration:
    - Number of chains: 20
    - Chain length: 2-5
    - File overlap threshold: 0.0
    - Time window: 6 months
    - Cache directory: .swebench_cache
    - Use cache: True

INFO - Building dependency DAG from 500 task instances
INFO - Loaded DAG from cache: .swebench_cache/dag_cache_a3f5b8c9d2e1f4a7.pkl
INFO - Using cached DAG computation
INFO - Sampled 20 diverse chains from DAG

✅ Successfully created 20 chains
   Saved to: data/chains_v2.jsonl

Time elapsed: 3 seconds
```

**Result**: Completed in 3 seconds (vs 5 minutes)! The DAG was loaded from cache, only sampling was performed.

## Third Run (Different DAG Parameters)

Now you want to try a stricter file overlap threshold:

```bash
$ python swebench/collect/build_chains_pipeline.py \
    --input_file data/my_instances.jsonl \
    --output_file data/chains_v3.jsonl \
    --num_chains 10 \
    --file_overlap_threshold 0.5

Building chains from task instances...
  Input file: data/my_instances.jsonl
  Output file: data/chains_v3.jsonl
  Configuration:
    - Number of chains: 10
    - Chain length: 2-5
    - File overlap threshold: 0.5
    - Time window: 6 months
    - Cache directory: .swebench_cache
    - Use cache: True

INFO - Building dependency DAG from 500 task instances
INFO - Analyzing PR dependencies: 100%|████████████| 500/500
INFO - DAG built with 500 nodes and 287 edges
INFO - Saved DAG to cache: .swebench_cache/dag_cache_7c4d9a2b8e3f1c6d.pkl
INFO - Sampled 10 diverse chains from DAG

✅ Successfully created 10 chains
   Saved to: data/chains_v3.jsonl

Time elapsed: 5 minutes 18 seconds
```

**Result**: A new DAG was computed because the `file_overlap_threshold` parameter changed. Both caches are now stored.

## Cache Management

### View Cache Files

```bash
$ ls -lh .swebench_cache/
total 24M
-rw-r--r-- 1 user user 12M Jan 15 10:23 dag_cache_a3f5b8c9d2e1f4a7.pkl
-rw-r--r-- 1 user user 11M Jan 15 10:35 dag_cache_7c4d9a2b8e3f1c6d.pkl
```

### Clear Cache

```bash
$ python swebench/collect/clear_dag_cache.py

Clearing DAG cache from: .swebench_cache
INFO - Removed cache file: .swebench_cache/dag_cache_a3f5b8c9d2e1f4a7.pkl
INFO - Removed cache file: .swebench_cache/dag_cache_7c4d9a2b8e3f1c6d.pkl
INFO - Cleared 2 cache file(s) from .swebench_cache
Done!
```

## Key Takeaways

1. **First run** computes DAG and saves to cache (~5 minutes)
2. **Subsequent runs** with same DAG parameters load from cache (~3 seconds)
3. **Different DAG parameters** create new cache entries
4. **Cache is automatic** - enabled by default, no configuration needed
5. **Easy to manage** - simple script to clear when needed

## When to Use Cache vs No Cache

### Use Cache (default)
- Experimenting with different `num_chains` values
- Trying different `min_chain_length` / `max_chain_length`
- Testing different sampling strategies
- Re-running after interruption

### Use `--no_cache`
- You've modified the input data
- You suspect the cache is corrupted
- You want to ensure fresh computation
- You're debugging the DAG construction itself

## Python API Example

```python
from swebench.collect.chain_construction import (
    build_chains_from_repository_data,
    load_task_instances,
)

# Load instances
task_instances = load_task_instances("data/my_instances.jsonl")

# First run - computes and caches DAG
print("Building with 10 chains...")
chains_10 = build_chains_from_repository_data(
    task_instances,
    num_chains=10,
    use_cache=True
)
# Takes ~5 minutes

# Second run - loads from cache
print("Building with 20 chains...")
chains_20 = build_chains_from_repository_data(
    task_instances,
    num_chains=20,
    use_cache=True
)
# Takes ~3 seconds!

# Third run - force recomputation
print("Rebuilding from scratch...")
chains_fresh = build_chains_from_repository_data(
    task_instances,
    num_chains=10,
    use_cache=False
)
# Takes ~5 minutes
```

## Performance Comparison

| Dataset Size | DAG Computation | Cached Load | Speedup |
|-------------|-----------------|-------------|---------|
| 100 instances | ~45 seconds | ~1 second | 45x |
| 500 instances | ~5 minutes | ~3 seconds | 100x |
| 1000 instances | ~18 minutes | ~8 seconds | 135x |
| 2000 instances | ~60 minutes | ~20 seconds | 180x |

*Note: Times are approximate and depend on hardware and dataset characteristics.*
