#!/usr/bin/env python3
"""
Diagnostic script to check commit information in task instances.

This script analyzes task instances to understand why the commit-to-PR map
might be empty or sparse.
"""

import argparse
import json
import sys


def analyze_task_instances(input_file: str, num_samples: int = 20):
    """
    Analyze task instances to check commit information.
    
    Args:
        input_file: Path to JSONL file containing task instances
        num_samples: Number of samples to display (default: 20)
    """
    print(f"Analyzing task instances from: {input_file}\n")
    
    total_instances = 0
    missing_head = 0
    missing_base = 0
    same_commits = 0
    samples = []
    
    with open(input_file, 'r') as f:
        for line in f:
            total_instances += 1
            instance = json.loads(line)
            
            pr_num = instance.get('pull_number')
            base = instance.get('base_commit')
            head = instance.get('head_commit')
            
            # Track statistics
            if not head:
                missing_head += 1
            if not base:
                missing_base += 1
            if base and head and base == head:
                same_commits += 1
            
            # Collect samples
            if len(samples) < num_samples:
                samples.append({
                    'pr': pr_num,
                    'base': base[:12] if base else 'MISSING',
                    'head': head[:12] if head else 'MISSING',
                    'same': base == head if (base and head) else False
                })
    
    # Print statistics
    print("="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(f"Total task instances: {total_instances}")
    print(f"Missing head_commit: {missing_head} ({missing_head/total_instances*100:.1f}%)")
    print(f"Missing base_commit: {missing_base} ({missing_base/total_instances*100:.1f}%)")
    print(f"base_commit == head_commit: {same_commits} ({same_commits/total_instances*100:.1f}%)")
    print()
    
    # Print samples
    print("="*60)
    print(f"SAMPLE PRs (first {len(samples)})")
    print("="*60)
    for sample in samples:
        print(f"PR {sample['pr']}:")
        print(f"  base: {sample['base']}")
        print(f"  head: {sample['head']}")
        print(f"  same: {sample['same']}")
        print()
    
    # Diagnosis
    print("="*60)
    print("DIAGNOSIS")
    print("="*60)
    
    if missing_head > total_instances * 0.5:
        print("⚠️  CRITICAL: More than 50% of instances are missing head_commit!")
        print("   This will result in an empty commit-to-PR map.")
        print("   Solution: Re-run data collection with head_commit extraction.")
    elif same_commits > total_instances * 0.5:
        print("⚠️  CRITICAL: More than 50% of instances have base_commit == head_commit!")
        print("   This means git log base..head returns no commits.")
        print("   Solution: Change commit-to-PR mapping strategy to use the patch itself")
        print("   or include head_commit in the map directly.")
    elif missing_head > 0:
        print(f"⚠️  WARNING: {missing_head} instances missing head_commit")
        print("   This will reduce the effectiveness of blame-based dependencies.")
    elif same_commits > 0:
        print(f"ℹ️  INFO: {same_commits} instances have base_commit == head_commit")
        print("   These PRs contribute no commits to the map (single-commit PRs).")
    else:
        print("✅ Commit data looks good! The problem may be elsewhere.")
        print("   Check if git log is working correctly in the repo.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "input_file",
        help="Path to JSONL file containing task instances"
    )
    parser.add_argument(
        "--num_samples",
        type=int,
        default=20,
        help="Number of sample PRs to display (default: 20)"
    )
    
    args = parser.parse_args()
    
    try:
        analyze_task_instances(args.input_file, args.num_samples)
    except FileNotFoundError:
        print(f"Error: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
