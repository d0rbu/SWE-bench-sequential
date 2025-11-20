"""
DAG-based dependency analysis for PR chains.

This module implements sophisticated dependency detection between PRs using:
- Git blame analysis on modified/deleted lines
- Temporal proximity filtering  
- Issue relationship matching
- File overlap detection

The result is a Directed Acyclic Graph (DAG) of dependencies from which
diverse chains can be sampled.
"""

import logging
import os
import random
import re
import subprocess
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import unidiff

logger = logging.getLogger(__name__)


# Type alias for PR sampler function
# Takes: (leaf_prs: List[int], dag: DependencyDAG, covered_files: Set[str], seed: Optional[int]) -> int
PRSampler = Callable[[List[int], "DependencyDAG", Set[str], Optional[int]], int]


@dataclass
class PRNode:
    """Represents a PR in the dependency DAG."""
    pr_number: int
    instance_id: str
    created_at: datetime
    base_commit: str
    issues: Set[str]
    modified_files: Set[str]
    modified_deleted_lines: Dict[str, Set[int]]  # file -> line numbers
    task_instance: Dict[str, Any]


@dataclass
class DependencyDAG:
    """Directed Acyclic Graph of PR dependencies."""
    nodes: Dict[int, PRNode] = field(default_factory=dict)
    edges: Dict[int, Dict[int, float]] = field(default_factory=dict)  # pr -> {dependency -> weight}
    
    def add_node(self, node: PRNode) -> None:
        """Add a PR node to the DAG."""
        self.nodes[node.pr_number] = node
        if node.pr_number not in self.edges:
            self.edges[node.pr_number] = {}
    
    def add_edge(self, from_pr: int, to_pr: int, weight: float) -> None:
        """Add a dependency edge with weight."""
        if from_pr not in self.edges:
            self.edges[from_pr] = {}
        self.edges[from_pr][to_pr] = weight
    
    def get_dependencies(self, pr_number: int) -> List[int]:
        """Get PRs that this PR depends on."""
        return list(self.edges.get(pr_number, {}).keys())
    
    def get_dependency_weights(self, pr_number: int) -> Dict[int, float]:
        """Get PRs that this PR depends on with their weights."""
        assert pr_number in self.edges, f"PR {pr_number} not found in DAG edges"
        return self.edges[pr_number]
    
    def get_topological_order(self) -> List[int]:
        """Return PRs in topological order (dependencies before dependents)."""
        in_degree = {pr: 0 for pr in self.nodes}
        for deps in self.edges.values():
            for dep in deps.keys():
                in_degree[dep] += 1
        
        queue = [pr for pr, deg in in_degree.items() if deg == 0]
        result = []
        
        while queue:
            pr = queue.pop(0)
            result.append(pr)
            for dep in self.edges.get(pr, {}).keys():
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)
        
        return result


def parse_patch_for_modified_deleted_lines(patch_str: str) -> Dict[str, Set[int]]:
    """
    Parse unified diff to extract modified/deleted line numbers.
    
    Returns a dict mapping file paths to line numbers that were modified/deleted.
    For renamed files, uses the SOURCE (old) filename since that's what exists at base_commit.
    
    NOTE: We explicitly exclude added lines - they can't have prior blame.
    NOTE: We skip newly created files - they have no prior history to blame.
    """
    if not patch_str or not patch_str.strip():
        return {}
    
    result = defaultdict(set)
    
    try:
        patch_set = unidiff.PatchSet(patch_str)
        for patched_file in patch_set:
            # Skip new files (they have no prior history to blame)
            if patched_file.is_added_file:
                continue
            
            # For renamed files, use the source (old) filename for blame
            # since that's what exists at the base commit
            if patched_file.is_rename:
                file_path = patched_file.source_file[2:] if patched_file.source_file.startswith('a/') else patched_file.source_file
            else:
                file_path = patched_file.path
            for hunk in patched_file:
                for line in hunk:
                    # Only removed lines (is_removed = True)
                    # Modified lines don't exist in unidiff - they're remove+add
                    if line.is_removed and line.source_line_no:
                        result[file_path].add(line.source_line_no)
        return dict(result)
    except Exception as e:
        logger.warning(f"Failed to parse patch with unidiff: {e}, falling back to manual parsing")
        
    # Fallback manual parsing
    result.clear()
    current_file = None
    is_new_file = False
    source_line = 1
    
    for line in patch_str.split('\n'):
        if line.startswith('--- '):
            current_file = None
            is_new_file = False
            # Check if this is a new file (--- /dev/null)
            if line.startswith('--- /dev/null'):
                is_new_file = True
            elif line.startswith('--- a/'):
                # Use source filename (this is what exists at base commit)
                current_file = line[6:].strip()
        elif line.startswith('+++ b/'):
            if is_new_file:
                # This is a new file, skip it
                current_file = None
                continue
            # For renamed files, current_file is already set to source filename
            # Only update if we don't have a source filename yet
            if not current_file:
                current_file = line[6:].strip()
        elif line.startswith('@@'):
            match = re.match(r'@@ -(\d+),?(\d*) \+(\d+),?(\d*) @@', line)
            if match:
                source_line = int(match.group(1))
                # If source line is 0, this is a new file
                if source_line == 0:
                    current_file = None
        elif current_file and line.startswith('-') and not line.startswith('---'):
            # Deleted line - record it
            result[current_file].add(source_line)
            source_line += 1
        elif current_file and not line.startswith('+'):
            # Context line
            source_line += 1
    
    return dict(result)


def extract_modified_files(patch_str: str) -> Set[str]:
    """Extract modified file paths from a patch."""
    if not patch_str:
        return set()
    
    files = set()
    
    try:
        patch_set = unidiff.PatchSet(patch_str)
        for patched_file in patch_set:
            files.add(patched_file.path)
        return files
    except Exception as e:
        logger.warning(f"Failed to parse patch: {e}, falling back to manual parsing")
    
    # Fallback
    for line in patch_str.split('\n'):
        if line.startswith('--- a/') or line.startswith('+++ b/'):
            files.add(line[6:].strip())
    
    return files


def is_valid_commit_sha(commit_sha: str) -> bool:
    """
    Validate that a string is a valid git commit SHA.
    Accepts both full (40 chars) and abbreviated (7-40 chars) SHAs.
    
    Args:
        commit_sha: String to validate
        
    Returns:
        True if valid commit SHA, False otherwise
    """
    # Git abbreviated SHAs are typically 7-11 characters, but can be up to 40
    # Accept any hex string between 7 and 40 characters
    return 7 <= len(commit_sha) <= 40 and all(c in '0123456789abcdef' for c in commit_sha.lower())


def git_blame_lines(
    repo_path: str, 
    file_path: str, 
    commit: str, 
    start_line: int, 
    end_line: int | None = None
) -> Dict[int, str]:
    """
    Run git blame to find which commits last modified a range of lines.
    
    Args:
        repo_path: Path to git repository
        file_path: Relative path to file within repo
        commit: Commit SHA to blame at (typically base_commit of the PR)
        start_line: Starting line number (1-indexed)
        end_line: Ending line number (1-indexed), or None to blame just start_line
        
    Returns:
        Dict mapping line numbers to commit SHAs
        
    Raises:
        RuntimeError: If git blame fails
    """
    if end_line is None:
        end_line = start_line
    
    try:
        # Use git blame with -L to blame line range
        result = subprocess.run(
            ['git', 'blame', '-L', f'{start_line},{end_line}', commit, '--', file_path],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode != 0:
            # Check if error is due to file not existing at this commit
            stderr = result.stderr.strip()
            if "no such path" in stderr or "does not exist" in stderr:
                raise RuntimeError(
                    f"File {file_path} does not exist at commit {commit}"
                )
            raise RuntimeError(
                f"Git blame failed for {file_path}:{start_line}-{end_line}: {stderr}"
            )
        
        # Parse output: "commit_sha (author date time linenum) line content"
        output = result.stdout.strip()
        if not output:
            raise RuntimeError(
                f"Git blame returned empty output for {file_path}:{start_line}-{end_line}"
            )
        
        blame_map = {}
        for line in output.split('\n'):
            if not line:
                continue
            # Extract commit SHA (first token)
            commit_sha = line.split()[0]
            # Remove leading ^ if present (means line existed in initial commit)
            commit_sha = commit_sha.lstrip('^')
            
            # Validate commit SHA format (accept both full and abbreviated SHAs)
            if not is_valid_commit_sha(commit_sha):
                logger.warning(f"Skipping invalid commit SHA format: {commit_sha}")
                continue
            
            # Extract line number from blame output
            # Format: "commit_sha (author date time linenum) line content"
            match = re.search(r'\(.*?\s+(\d+)\)', line)
            if match:
                line_num = int(match.group(1))
                blame_map[line_num] = commit_sha
        
        return blame_map
        
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Git blame timed out for {file_path}:{start_line}-{end_line}") from e
    except Exception as e:
        raise RuntimeError(f"Git blame failed for {file_path}:{start_line}-{end_line}: {e}") from e

def _fetch_commit_if_missing(repo_path: str, commit_sha: str) -> bool:
    """
    Check if a commit exists in the repo, and fetch it from remote if missing.
    
    Args:
        repo_path: Path to git repository
        commit_sha: Commit SHA to check/fetch
        
    Returns:
        True if commit exists or was successfully fetched, False otherwise
    """
    # Check if commit exists
    check = subprocess.run(
        ['git', 'cat-file', '-e', commit_sha],
        cwd=repo_path,
        capture_output=True,
        timeout=5
    )
    
    if check.returncode == 0:
        return True  # Commit already exists
    
    # Try to fetch the commit from origin
    logger.debug(f"Fetching missing commit {commit_sha} from remote")
    fetch = subprocess.run(
        ['git', 'fetch', 'origin', commit_sha],
        cwd=repo_path,
        capture_output=True,
        text=True,
        timeout=30
    )
    
    if fetch.returncode != 0:
        logger.debug(f"Failed to fetch commit {commit_sha}: {fetch.stderr.strip()}")
        return False
    
    # Verify it now exists
    check = subprocess.run(
        ['git', 'cat-file', '-e', commit_sha],
        cwd=repo_path,
        capture_output=True,
        timeout=5
    )
    
    return check.returncode == 0


def build_commit_to_pr_map(pr_nodes: Dict[int, PRNode], repo_path: str) -> Dict[str, int]:
    """
    Build a mapping from commit SHA to PR number for all PRs.
    
    This uses git log to find all commits in each PR's history.
    Fetches missing commits from remote if needed.
    
    Args:
        pr_nodes: All PR nodes in the DAG
        repo_path: Path to git repository
        
    Returns:
        Dict mapping commit SHA to PR number
    """
    commit_to_pr = {}
    
    for pr_number, node in pr_nodes.items():
        try:
            # Get all commits from base_commit to the PR's head
            # We use the task_instance to get the head commit
            head_commit = node.task_instance.get('head_commit')
            assert head_commit, f"PR {pr_number} missing head_commit"
            
            # Ensure both base and head commits exist, fetching if needed
            for commit_sha, commit_type in [(node.base_commit, 'base'), (head_commit, 'head')]:
                if not _fetch_commit_if_missing(repo_path, commit_sha):
                    raise RuntimeError(f"{commit_type}_commit {commit_sha} not found in repo and couldn't be fetched")
            
            # Use git log to get all commits in this PR
            result = subprocess.run(
                ['git', 'log', '--format=%H', f'{node.base_commit}..{head_commit}'],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            assert result.returncode == 0, f"git log failed for PR {pr_number}: {result.stderr}"
            
            commits = result.stdout.strip().split('\n')
            for commit_sha in commits:
                if not commit_sha:
                    continue
                    
                if not is_valid_commit_sha(commit_sha):
                    logger.error(f"Invalid commit SHA format in PR {pr_number}: {commit_sha}")
                    continue
                    
                commit_to_pr[commit_sha] = pr_number
        except Exception as e:
            logger.error(f"Failed to get commits for PR {pr_number}: {e}")
            continue
    
    return commit_to_pr


def build_blame_counter_for_pr(
    pr_node: PRNode,
    repo_path: str
) -> Tuple[Counter[str], int]:
    """
    Build a counter of commit SHAs for all modified/deleted lines in a PR.
    
    This runs git blame once for each file (or contiguous line ranges) and aggregates
    the results into a counter showing how many lines blame to each commit.
    
    Args:
        pr_node: PR node to analyze
        repo_path: Path to git repository
        
    Returns:
        Tuple of (Counter mapping commit SHA to line count, total lines analyzed)
    """
    blame_counter = Counter()
    total_lines = 0
    
    # For each file with modified/deleted lines
    for file_path, line_nums in pr_node.modified_deleted_lines.items():
        if not line_nums:
            continue
        
        # Convert set to sorted list for range grouping
        sorted_lines = sorted(line_nums)
        total_lines += len(sorted_lines)
        
        # Group consecutive lines into ranges for efficient git blame
        ranges = []
        start = sorted_lines[0]
        end = sorted_lines[0]
        
        for line_num in sorted_lines[1:]:
            if line_num == end + 1:
                # Extend current range
                end = line_num
            else:
                # Save current range and start new one
                ranges.append((start, end))
                start = line_num
                end = line_num
        ranges.append((start, end))
        
        # Run git blame for each range
        for start_line, end_line in ranges:
            try:
                blame_map = git_blame_lines(
                    repo_path,
                    file_path,
                    pr_node.base_commit,
                    start_line,
                    end_line
                )
                # Count commits
                for commit_sha in blame_map.values():
                    blame_counter[commit_sha] += 1
            except RuntimeError as e:
                logger.warning(f"Failed to blame {file_path}:{start_line}-{end_line} for PR {pr_node.pr_number}: {e}")
                continue
    
    return blame_counter, total_lines


def calculate_blame_dependencies(
    pr_node: PRNode,
    all_pr_nodes: Dict[int, PRNode],
    repo_path: str,
    commit_to_pr_map: Dict[str, int]
) -> Dict[int, float]:
    """
    Calculate blame-based dependencies for a PR against all other PRs.
    
    This uses a two-stage approach for efficiency:
    1. Build blame counter for all modified/deleted lines in the PR (once)
    2. Aggregate blame counts by PR and normalize to percentages
    
    Args:
        pr_node: PR to analyze dependencies for
        all_pr_nodes: All PR nodes in the DAG
        repo_path: Path to git repository
        commit_to_pr_map: Mapping of commit SHAs to PR numbers
        
    Returns:
        Dict mapping PR numbers to blame dependency percentages (0.0 to 1.0)
    """
    # Stage 1: Build blame counter for this PR
    blame_counter, total_lines = build_blame_counter_for_pr(pr_node, repo_path)
    
    if total_lines == 0:
        logger.debug(f"PR {pr_node.pr_number}: No modified/deleted lines to blame")
        return {}
    
    logger.debug(f"PR {pr_node.pr_number}: Blamed {total_lines} lines to {len(blame_counter)} unique commits")
    
    # Stage 2: Aggregate blame counts by PR
    pr_blame_counts = defaultdict(int)
    commits_in_map = 0
    commits_not_in_map = 0
    
    for commit_sha, line_count in blame_counter.items():
        # Look up which PR this commit belongs to
        if commit_sha in commit_to_pr_map:
            commits_in_map += 1
            blamed_pr = commit_to_pr_map[commit_sha]
            # This should never happen - blame is done at base commit
            assert blamed_pr != pr_node.pr_number, \
                f"PR {pr_node.pr_number} blamed to itself for commit {commit_sha}"
            pr_blame_counts[blamed_pr] += line_count
        else:
            commits_not_in_map += 1
    
    if commits_not_in_map > 0:
        logger.debug(f"PR {pr_node.pr_number}: {commits_in_map} commits found in map, {commits_not_in_map} not found (likely older PRs not in dataset)")
    
    # Stage 3: Normalize to percentages
    blame_percentages = {
        pr: count / total_lines
        for pr, count in pr_blame_counts.items()
    }
    
    return blame_percentages


def build_dependency_dag(
    task_instances: List[Dict[str, Any]],
    repo_path: str,
    time_window_months: int = 6,
    blame_threshold: float = 0.05
) -> DependencyDAG:
    """
    Build a dependency DAG from task instances using git blame analysis.
    
    Algorithm:
    1. Sort PRs by date (newest to oldest)
    2. For each PR, examine all earlier PRs:
       a. Same issue → automatic dependency
       b. >6 months old → skip
       c. No file overlap → skip  
       d. Otherwise → calculate blame percentage
       e. If blame % > threshold → add dependency
    
    Args:
        task_instances: List of task instance dictionaries
        repo_path: Path to git repository for blame analysis (required)
        time_window_months: Maximum age difference for dependencies (default: 6)
        blame_threshold: Minimum blame percentage for dependency (default: 0.05 = 5%)
        
    Returns:
        DependencyDAG with nodes and weighted edges
    """
    # Assert repo_path is provided and exists
    assert repo_path, "repo_path must be provided"
    assert os.path.exists(repo_path), f"repo_path does not exist: {repo_path}"
    
    dag = DependencyDAG()
    
    # Parse task instances into PR nodes
    pr_nodes = []
    for instance in task_instances:
        # Parse creation date
        created_at_str = instance.get('created_at', '')
        try:
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        except:
            created_at = datetime.now()
        
        # Parse patch to get modified files and lines
        patch = instance.get('patch', '')
        modified_files = extract_modified_files(patch)
        modified_deleted_lines = parse_patch_for_modified_deleted_lines(patch)
        
        # Extract issues
        issues = set(str(issue) for issue in instance.get('issue_numbers', []))
        
        node = PRNode(
            pr_number=instance['pull_number'],
            instance_id=instance['instance_id'],
            created_at=created_at,
            base_commit=instance.get('base_commit', ''),
            issues=issues,
            modified_files=modified_files,
            modified_deleted_lines=modified_deleted_lines,
            task_instance=instance
        )
        pr_nodes.append(node)
        dag.add_node(node)
    
    # Sort by date (newest first)
    pr_nodes.sort(key=lambda x: x.created_at, reverse=True)
    
    # Build commit to PR mapping
    logger.info("Building commit-to-PR mapping...")
    pr_node_dict = {pr.pr_number: pr for pr in pr_nodes}
    commit_to_pr_map = build_commit_to_pr_map(pr_node_dict, repo_path)
    logger.info(f"Mapped {len(commit_to_pr_map)} commits to {len(set(commit_to_pr_map.values()))} PRs")
    
    if len(commit_to_pr_map) == 0:
        logger.warning("WARNING: commit_to_pr_map is empty! No blame-based dependencies will be detected.")
    
    # Process each PR against all earlier PRs
    stats = {
        'total_comparisons': 0,
        'filtered_time': 0,
        'filtered_no_issues': 0,
        'filtered_no_file_overlap': 0,
        'issue_based': 0,
        'blame_checked': 0,
        'blame_below_threshold': 0,
        'blame_based': 0
    }
    
    for i, target_pr in enumerate(pr_nodes):
        if i % 100 == 0:
            logger.info(f"Progress: Analyzed {i}/{len(pr_nodes)} PRs")
        logger.debug(f"Analyzing dependencies for PR {target_pr.pr_number}")
        
        # Calculate blame dependencies for all earlier PRs
        blame_dependencies = calculate_blame_dependencies(
            target_pr,
            pr_node_dict,
            repo_path,
            commit_to_pr_map
        )
        logger.debug(f"  Found {len(blame_dependencies)} PRs with blame dependencies")
        
        # Look at all earlier PRs (later in list due to reverse sort)
        candidates_in_window = 0
        for candidate_pr in pr_nodes[i+1:]:
            stats['total_comparisons'] += 1
            
            # Filter 1: Check temporal proximity
            age_diff = target_pr.created_at - candidate_pr.created_at
            if age_diff.days > time_window_months * 30:
                stats['filtered_time'] += 1
                continue
            
            candidates_in_window += 1
            
            # Filter 2: Check for shared issues (automatic dependency)
            if target_pr.issues and candidate_pr.issues:
                if target_pr.issues & candidate_pr.issues:
                    dag.add_edge(target_pr.pr_number, candidate_pr.pr_number, 1.0)
                    logger.info(f"  → Issue-based dependency on PR {candidate_pr.pr_number}")
                    stats['issue_based'] += 1
                    continue
            
            # Filter 3: Check for file overlap
            if not (target_pr.modified_files & candidate_pr.modified_files):
                stats['filtered_no_file_overlap'] += 1
                continue
            
            # Check if we have blame dependency for this candidate
            stats['blame_checked'] += 1
            if candidate_pr.pr_number in blame_dependencies:
                blame_pct = blame_dependencies[candidate_pr.pr_number]
                if blame_pct >= blame_threshold:
                    dag.add_edge(target_pr.pr_number, candidate_pr.pr_number, blame_pct)
                    logger.info(f"  → Blame-based dependency on PR {candidate_pr.pr_number} ({blame_pct:.1%})")
                    stats['blame_based'] += 1
                else:
                    stats['blame_below_threshold'] += 1
                    logger.debug(f"  Blame dependency on PR {candidate_pr.pr_number} below threshold: {blame_pct:.1%}")
        
        if candidates_in_window > 0:
            logger.debug(f"  Checked {candidates_in_window} candidates within time window")
    
    # Log final statistics
    logger.info("\n" + "="*60)
    logger.info("DAG Construction Statistics:")
    logger.info(f"  Total PR comparisons: {stats['total_comparisons']:,}")
    logger.info(f"  Filtered by time window: {stats['filtered_time']:,}")
    logger.info(f"  Filtered by no file overlap: {stats['filtered_no_file_overlap']:,}")
    logger.info(f"  Issue-based dependencies found: {stats['issue_based']}")
    logger.info(f"  Blame checks performed: {stats['blame_checked']:,}")
    logger.info(f"  Blame-based dependencies found: {stats['blame_based']}")
    logger.info(f"  Blame below threshold: {stats['blame_below_threshold']:,}")
    logger.info(f"  Commit-to-PR map size: {len(commit_to_pr_map):,} commits")
    logger.info("="*60 + "\n")
    
    return dag


def file_coverage_sampler(leaf_prs: List[int], dag: DependencyDAG, covered_files: Set[str], seed: Optional[int] = None) -> int:
    """
    Sample PR that maximizes file coverage diversity.
    
    Picks the PR that touches the most files not yet covered by selected chains.
    If there are ties, randomly selects from PRs with maximum uncovered files.
    
    Args:
        leaf_prs: List of PR numbers to sample from
        dag: Dependency DAG containing PR nodes
        covered_files: Set of file paths already covered by previous chains
        seed: Random seed for deterministic tie-breaking
        
    Returns:
        Selected PR number that maximizes uncovered files
    """
    # Calculate uncovered file counts for all PRs
    uncovered_counts = [
        len(dag.nodes[pr].modified_files - covered_files) 
        for pr in leaf_prs
    ]
    
    # Find maximum uncovered file count
    max_uncovered = max(uncovered_counts)
    
    # Get all PRs with maximum uncovered files
    best_prs = [
        pr for pr, count in zip(leaf_prs, uncovered_counts) 
        if count == max_uncovered
    ]
    
    # Randomly select from best PRs (with optional seed for determinism)
    if seed is not None:
        random.seed(seed)
    return random.choice(best_prs)


def random_sampler(leaf_prs: List[int], dag: DependencyDAG, covered_files: Set[str], seed: Optional[int] = None) -> int:
    """
    Random PR selection for baseline comparison.
    
    Args:
        leaf_prs: List of PR numbers to sample from
        dag: Dependency DAG containing PR nodes (unused but kept for interface consistency)
        covered_files: Set of file paths already covered (unused but kept for interface consistency)
        seed: Random seed for deterministic sampling
        
    Returns:
        Randomly selected PR number
    """
    if seed is not None:
        random.seed(seed)
    return random.choice(leaf_prs)


def sample_chains_from_dag(
    dag: DependencyDAG,
    num_chains: int = 10,
    min_chain_length: int = 2,
    max_chain_length: int = 5,
    sampler: PRSampler = file_coverage_sampler,
    seed: Optional[int] = None
) -> List[List[Dict[str, Any]]]:
    """
    Sample diverse chains from the dependency DAG.
    
    Args:
        dag: DependencyDAG to sample from
        num_chains: Number of chains to sample
        min_chain_length: Minimum chain length
        max_chain_length: Maximum chain length
        sampler: Function to select starting PR for each chain.
                 Takes (leaf_prs, dag, covered_files, seed) and returns selected PR number.
                 Defaults to file_coverage_sampler.
        seed: Random seed for deterministic sampling
        
    Returns:
        List of chains, where each chain is a list of task instances in dependency order
    """
    chains = []
    used_prs = set()
    covered_files = set()
    
    # Get PRs in topological order (dependencies first)
    topo_order = dag.get_topological_order()
    
    # Start from PRs with no dependencies (leaf nodes in dep sense)
    leaf_prs = [pr for pr in topo_order if not dag.get_dependencies(pr)]
    
    for _ in range(num_chains):
        if not leaf_prs:
            break
        
        # Use the injected sampler to pick starting PR
        best_pr = sampler(leaf_prs, dag, covered_files, seed)
        
        # Build chain by following dependencies
        chain = []
        current_pr = best_pr
        
        while current_pr and len(chain) < max_chain_length:
            node = dag.nodes[current_pr]
            chain.append(node.task_instance)
            used_prs.add(current_pr)
            covered_files.update(node.modified_files)
            
            # Follow strongest dependency
            deps = dag.get_dependencies(current_pr)
            if deps:
                # Sort by weight and pick strongest
                dep_weights = dag.get_dependency_weights(current_pr)
                best_dep = max(deps, key=lambda pr: dep_weights.get(pr, 0))
                current_pr = best_dep
            else:
                break
        
        # Only add if meets minimum length
        if len(chain) >= min_chain_length:
            # Reverse so it goes from oldest to newest (dependency order)
            chains.append(list(reversed(chain)))
            logger.info(f"Sampled chain: {[inst['pull_number'] for inst in chain]}")
    
    return chains
