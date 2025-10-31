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
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import unidiff

logger = logging.getLogger(__name__)


# Type alias for PR sampler function
# Takes: (leaf_prs: List[int], dag: DependencyDAG, covered_files: Set[str]) -> int
PRSampler = Callable[[List[int], "DependencyDAG", Set[str]], int]


@dataclass
class PRNode:
    """Represents a PR in the dependency DAG."""
    pr_number: int
    instance_id: str
    created_at: datetime
    base_commit: str
    issues: Set[str]
    modified_files: Set[str]
    modified_deleted_lines: Dict[str, List[int]]  # file -> line numbers
    task_instance: Dict[str, Any]
    dependencies: Dict[int, float] = field(default_factory=dict)  # pr_number -> blame %


@dataclass
class DependencyDAG:
    """Directed Acyclic Graph of PR dependencies."""
    nodes: Dict[int, PRNode] = field(default_factory=dict)
    edges: Dict[int, Set[int]] = field(default_factory=dict)
    
    def add_node(self, node: PRNode) -> None:
        """Add a PR node to the DAG."""
        self.nodes[node.pr_number] = node
        if node.pr_number not in self.edges:
            self.edges[node.pr_number] = set()
    
    def add_edge(self, from_pr: int, to_pr: int, weight: float) -> None:
        """Add a dependency edge."""
        if from_pr not in self.edges:
            self.edges[from_pr] = set()
        self.edges[from_pr].add(to_pr)
        self.nodes[from_pr].dependencies[to_pr] = weight
    
    def get_dependencies(self, pr_number: int) -> List[int]:
        """Get PRs that this PR depends on."""
        return list(self.edges.get(pr_number, set()))
    
    def get_topological_order(self) -> List[int]:
        """Return PRs in topological order (dependencies before dependents)."""
        in_degree = {pr: 0 for pr in self.nodes}
        for deps in self.edges.values():
            for dep in deps:
                in_degree[dep] += 1
        
        queue = [pr for pr, deg in in_degree.items() if deg == 0]
        result = []
        
        while queue:
            pr = queue.pop(0)
            result.append(pr)
            for dep in self.edges.get(pr, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)
        
        return result


def parse_patch_for_modified_deleted_lines(patch_str: str) -> Dict[str, List[int]]:
    """
    Parse unified diff to extract modified/deleted line numbers.
    
    NOTE: We explicitly exclude added lines - they can't have prior blame.
    """
    if not patch_str or not patch_str.strip():
        return {}
    
    result = defaultdict(list)
    
    try:
        patch_set = unidiff.PatchSet(patch_str)
        for patched_file in patch_set:
            file_path = patched_file.path
            for hunk in patched_file:
                for line in hunk:
                    # Only removed lines (is_removed = True)
                    # Modified lines don't exist in unidiff - they're remove+add
                    if line.is_removed and line.source_line_no:
                        result[file_path].append(line.source_line_no)
        return dict(result)
    except Exception as e:
        logger.warning(f"Failed to parse patch with unidiff: {e}, falling back to manual parsing")
        
    # Fallback manual parsing
    result.clear()
    current_file = None
    source_line = 1
    
    for line in patch_str.split('\n'):
        if line.startswith('--- a/'):
            current_file = line[6:].strip()
        elif line.startswith('+++ b/'):
            if not current_file:
                current_file = line[6:].strip()
        elif line.startswith('@@'):
            match = re.match(r'@@ -(\d+),?(\d*) \+(\d+),?(\d*) @@', line)
            if match:
                source_line = int(match.group(1))
        elif current_file and line.startswith('-') and not line.startswith('---'):
            # Deleted line - record it
            result[current_file].append(source_line)
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


def git_blame_line(repo_path: str, file_path: str, line_num: int, commit: str) -> Optional[str]:
    """
    Run git blame to find which commit last modified a specific line.
    
    Args:
        repo_path: Path to git repository
        file_path: Relative path to file within repo
        line_num: Line number to blame (1-indexed)
        commit: Commit SHA to blame at (typically base_commit of the PR)
        
    Returns:
        Commit SHA that last modified this line, or None if blame fails
    """
    try:
        # Use git blame with -L to blame just one line
        result = subprocess.run(
            ['git', 'blame', '-L', f'{line_num},{line_num}', commit, '--', file_path],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode != 0:
            return None
        
        # Parse output: "commit_sha (author date time linenum) line content"
        output = result.stdout.strip()
        if output:
            # Extract commit SHA (first token)
            commit_sha = output.split()[0]
            # Remove leading ^ if present (means line existed in initial commit)
            return commit_sha.lstrip('^')
        
        return None
    except Exception as e:
        logger.debug(f"Git blame failed for {file_path}:{line_num}: {e}")
        return None


def map_commit_to_pr(commit_sha: str, pr_nodes: Dict[int, PRNode], repo_path: str) -> Optional[int]:
    """
    Map a commit SHA to a PR number.
    
    This checks if the commit is part of the PR's merge commit or any commits
    in the PR's history.
    
    Args:
        commit_sha: Commit SHA to map
        pr_nodes: All PR nodes in the DAG
        repo_path: Path to git repository
        
    Returns:
        PR number if found, None otherwise
    """
    # TODO: This is a simplified implementation
    # In practice, you'd need to:
    # 1. Get all commits in each PR (from base to head)
    # 2. Check if commit_sha is in that range
    # 3. Cache this mapping for performance
    
    # For now, we'll just return None and rely on other heuristics
    # A full implementation would query git log for each PR's commit range
    return None


def calculate_blame_dependency(
    target_pr: PRNode,
    candidate_pr: PRNode,
    repo_path: str,
    commit_to_pr_map: Dict[str, int]
) -> float:
    """
    Calculate what percentage of target PR's modified/deleted lines
    trace back to candidate PR via git blame.
    
    Args:
        target_pr: The newer PR we're analyzing
        candidate_pr: The older PR we're checking as a potential dependency
        repo_path: Path to git repository
        commit_to_pr_map: Mapping of commit SHAs to PR numbers
        
    Returns:
        Percentage (0.0 to 1.0) of target's modified/deleted lines that blame to candidate
    """
    total_lines = 0
    blamed_lines = 0
    
    # For each file modified/deleted in target PR
    for file_path, line_nums in target_pr.modified_deleted_lines.items():
        total_lines += len(line_nums)
        
        # For each modified/deleted line, run git blame at target's base commit
        for line_num in line_nums:
            blame_commit = git_blame_line(
                repo_path,
                file_path,
                line_num,
                target_pr.base_commit
            )
            
            if blame_commit:
                # Check if this commit belongs to candidate PR
                blamed_pr = commit_to_pr_map.get(blame_commit)
                if blamed_pr == candidate_pr.pr_number:
                    blamed_lines += 1
    
    if total_lines == 0:
        return 0.0
    
    return blamed_lines / total_lines


def build_dependency_dag(
    task_instances: List[Dict[str, Any]],
    repo_path: str,
    time_window_months: int = 6,
    blame_threshold: float = 0.1,
    commit_to_pr_map: Optional[Dict[str, int]] = None
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
        repo_path: Path to git repository for blame analysis
        time_window_months: Maximum age difference for dependencies (default: 6)
        blame_threshold: Minimum blame percentage for dependency (default: 0.1 = 10%)
        commit_to_pr_map: Optional mapping of commits to PRs (built if not provided)
        
    Returns:
        DependencyDAG with nodes and weighted edges
    """
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
    
    # Build commit to PR mapping if not provided
    if commit_to_pr_map is None:
        commit_to_pr_map = {}
        # TODO: Build this by iterating through PR commit ranges
        # For now, we'll work without it and rely on other heuristics
    
    # Process each PR against all earlier PRs
    for i, target_pr in enumerate(pr_nodes):
        logger.info(f"Analyzing dependencies for PR {target_pr.pr_number}")
        
        # Look at all earlier PRs (later in list due to reverse sort)
        for candidate_pr in pr_nodes[i+1:]:
            # Filter 1: Check temporal proximity
            age_diff = target_pr.created_at - candidate_pr.created_at
            if age_diff.days > time_window_months * 30:
                continue
            
            # Filter 2: Check for shared issues (automatic dependency)
            if target_pr.issues and candidate_pr.issues:
                if target_pr.issues & candidate_pr.issues:
                    dag.add_edge(target_pr.pr_number, candidate_pr.pr_number, 1.0)
                    logger.info(f"  → Issue-based dependency on PR {candidate_pr.pr_number}")
                    continue
            
            # Filter 3: Check for file overlap
            if not (target_pr.modified_files & candidate_pr.modified_files):
                continue
            
            # Git blame analysis
            if repo_path and os.path.exists(repo_path):
                blame_pct = calculate_blame_dependency(
                    target_pr,
                    candidate_pr,
                    repo_path,
                    commit_to_pr_map
                )
                
                if blame_pct >= blame_threshold:
                    dag.add_edge(target_pr.pr_number, candidate_pr.pr_number, blame_pct)
                    logger.info(f"  → Blame-based dependency on PR {candidate_pr.pr_number} ({blame_pct:.1%})")
    
    return dag


def file_coverage_sampler(leaf_prs: List[int], dag: DependencyDAG, covered_files: Set[str]) -> int:
    """
    Sample PR that maximizes file coverage diversity.
    
    Picks the PR that touches the most files not yet covered by selected chains.
    
    Args:
        leaf_prs: List of PR numbers to sample from
        dag: Dependency DAG containing PR nodes
        covered_files: Set of file paths already covered by previous chains
        
    Returns:
        Selected PR number that maximizes uncovered files
    """
    # Pick PR with most uncovered files
    best_pr = max(
        leaf_prs,
        key=lambda pr: len(dag.nodes[pr].modified_files - covered_files)
    )
    return best_pr


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
    sampler: PRSampler = file_coverage_sampler
) -> List[List[Dict[str, Any]]]:
    """
    Sample diverse chains from the dependency DAG.
    
    Args:
        dag: DependencyDAG to sample from
        num_chains: Number of chains to sample
        min_chain_length: Minimum chain length
        max_chain_length: Maximum chain length
        sampler: Function to select starting PR for each chain.
                 Takes (leaf_prs, dag, covered_files) and returns selected PR number.
                 Defaults to file_coverage_sampler.
        
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
        best_pr = sampler(leaf_prs, dag, covered_files)
        
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
                best_dep = max(deps, key=lambda pr: node.dependencies.get(pr, 0))
                current_pr = best_dep
            else:
                break
        
        # Only add if meets minimum length
        if len(chain) >= min_chain_length:
            # Reverse so it goes from oldest to newest (dependency order)
            chains.append(list(reversed(chain)))
            logger.info(f"Sampled chain: {[inst['pull_number'] for inst in chain]}")
    
    return chains
