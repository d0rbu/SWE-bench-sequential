# Summary of Changes: Resumable Chain Construction

## Overview

The chain construction pipeline has been enhanced to support **caching of the expensive DAG computation**. This allows you to resume work without having to wait for the entire computation to complete every time you run it.

## Key Changes

### 1. Modified Files

#### `swebench/collect/chain_construction.py`
- **Added imports**: `os`, `pickle`, `Path` for cache management
- **New functions**:
  - `_compute_dag_cache_key()`: Computes a cache key from task instances and parameters
  - `_get_dag_cache_path()`: Gets the path to the cached DAG file
  - `_save_dag_to_cache()`: Saves DAG to disk using pickle
  - `_load_dag_from_cache()`: Loads DAG from disk
  - `clear_dag_cache()`: Utility to clear all cached DAG files

- **Modified functions**:
  - `build_chains_from_repository_data()`: Now supports `cache_dir` and `use_cache` parameters
    - Checks cache before computing DAG
    - Saves computed DAG to cache for future use
  - `convert_single_instances_to_chains()`: Now accepts and passes through cache parameters

#### `swebench/collect/build_chains_pipeline.py`
- **Modified function** `main()`: Added two new parameters:
  - `cache_dir`: Directory to store cache files (default: `.swebench_cache`)
  - `use_cache`: Whether to use cached DAG if available (default: `True`)
- **New command-line arguments**:
  - `--cache_dir`: Specify custom cache directory
  - `--use_cache` / `--no_cache`: Enable/disable caching
- **Updated output**: Shows cache configuration in status messages

### 2. New Files

#### `swebench/collect/clear_dag_cache.py`
- Standalone script to clear cached DAG files
- Usage: `python swebench/collect/clear_dag_cache.py [--cache_dir PATH]`

#### `CACHING.md`
- Comprehensive documentation on the caching feature
- Explains how caching works, cache key computation, and usage examples
- Includes both CLI and Python API examples

#### `CHANGES_SUMMARY.md`
- This file - summary of all changes made

### 3. Configuration Files

#### `.gitignore`
- Added `.swebench_cache/` to ignore cached files from version control

## How Caching Works

### Cache Key Computation
The cache key is computed from:
1. All task instance IDs (sorted alphabetically)
2. `time_window_months` parameter
3. `file_overlap_threshold` parameter

This ensures that:
- Different input datasets get different caches
- Different DAG parameters get different caches
- Chain sampling parameters (`num_chains`, `min_chain_length`, etc.) don't affect the cache

### Cache Storage
- **Location**: `.swebench_cache/` (by default, configurable)
- **Format**: Python pickle files (`dag_cache_<hash>.pkl`)
- **Content**: Complete `DependencyDAG` object including all nodes and edges

### Performance Impact
- **First run**: Same as before (computes DAG from scratch)
- **Subsequent runs**: Loads from cache (typically seconds vs minutes/hours)
- **Cache invalidation**: Automatic when inputs or parameters change

## Usage Examples

### Basic Usage (caching enabled by default)
```bash
python swebench/collect/build_chains_pipeline.py \
    --input_file data/instances.jsonl \
    --output_file data/chains.jsonl
```

### Force Recomputation
```bash
python swebench/collect/build_chains_pipeline.py \
    --input_file data/instances.jsonl \
    --output_file data/chains.jsonl \
    --no_cache
```

### Custom Cache Directory
```bash
python swebench/collect/build_chains_pipeline.py \
    --input_file data/instances.jsonl \
    --output_file data/chains.jsonl \
    --cache_dir /tmp/my_cache
```

### Clear Cache
```bash
python swebench/collect/clear_dag_cache.py
```

## Python API

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

# Clear cache
clear_dag_cache()
```

## Backward Compatibility

All changes are **fully backward compatible**:
- Default behavior uses caching (opt-out with `--no_cache`)
- All existing parameters work exactly as before
- Existing scripts and code will work without modification
- Cache can be disabled with `use_cache=False`

## Testing

The implementation includes:
- Syntax validation (all files compile without errors)
- Proper error handling for cache failures
- Automatic cache directory creation
- Safe fallback to recomputation if cache is corrupted

## Benefits

1. **Time Savings**: Skip expensive DAG computation on subsequent runs
2. **Flexibility**: Experiment with different chain sampling parameters without recomputing DAG
3. **Transparency**: Clear logging shows whether cache is used or computed fresh
4. **Safety**: Cache failures automatically fall back to recomputation
5. **Maintainability**: Easy to clear cache when needed

## Notes

- Cache files can become large for big datasets (typically a few MB)
- Cache is stored as Python pickle, which is fast but not portable across Python versions
- The cache directory is automatically created if it doesn't exist
- Cache key uses MD5 hash (collision probability is negligible for this use case)
