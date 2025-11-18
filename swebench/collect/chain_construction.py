import json
from datetime import datetime
from tqdm import tqdm
import os
import subprocess
from tempfile import TemporaryDirectory
from unidiff import PatchSet
from swebench.collect.utils import Repo, extract_modified_files, build_dependency_graph, find_connected_components, extract_problem_statement_and_hints
from swebench.versioning.get_versions import get_version


def construct_chains_from_prs(prs_jsonl_path: str, output_path: str, target_chains: int = 10):
    """
    Main function to construct, score, and select the best issue chains.

    Args:
        prs_jsonl_path (str): Path to the .jsonl file with raw PR data.
        output_path (str): Path to save the final chains .jsonl file.
        target_chains (int): The number of top-quality chains to save for the pilot dataset.
    """
    with open(prs_jsonl_path, 'r') as f:
        prs = [json.loads(line) for line in f if line.strip()]
    
    if not prs:
        print("No pull requests found.")
        return

    repo_name = prs[0]['base']['repo']['full_name']
    owner, repo = repo_name.split("/")
    repo_obj = Repo(owner, repo)
    pr_map = {pr['number']: pr for pr in prs}
    print(f"Processing {len(prs)} PRs from {repo_name}...")

    prs_with_files = []
    for pr in tqdm(prs, desc="Fetching modified files"):
        pr['modified_files'] = extract_modified_files(pr, repo_obj)
        prs_with_files.append(pr)

    print("Building dependency graph...")
    dependency_graph = build_dependency_graph(prs_with_files)

    print("Finding connected components...")
    chains_of_numbers = find_connected_components(dependency_graph)
    multi_turn_chains = [chain for chain in chains_of_numbers if len(chain) > 1]
    print(f"Found {len(multi_turn_chains)} potential multi-turn chains.")

    # Score all potential chains
    scored_chains = []
    for chain_pr_numbers in multi_turn_chains:
        chain_prs = [pr_map[num] for num in chain_pr_numbers if num in pr_map]
        chain_prs.sort(key=lambda pr: datetime.fromisoformat(pr['created_at'].replace('Z', '+00:00')))
        
        score = score_chain_quality(chain_prs)
        if score > 0:  # Only consider valid (all merged) chains
            scored_chains.append({'pr_numbers': chain_pr_numbers, 'score': score})
    
    # Sort chains by score in descending order
    scored_chains.sort(key=lambda x: x['score'], reverse=True)
    print(f"Scored and validated {len(scored_chains)} chains.")

    # Select the top N chains for our pilot dataset
    top_chains_to_process = [c['pr_numbers'] for c in scored_chains[:target_chains]]

    # Process and enrich only the top-scoring chains
    final_chains = process_and_enrich_chains(top_chains_to_process, prs, repo_obj)

    with open(output_path, 'w') as f:
        for chain in final_chains:
            f.write(json.dumps(chain) + '\n')
    
    print(f"Saved {len(final_chains)} top-quality chains to {output_path}")


def process_and_enrich_chains(chains: list[list[int]], prs: list[dict], repo_obj: Repo) -> list[dict]:
    """
    Enriches, sorts, and converts chains of PR numbers into structured multi-turn task instances.
    (Corrected base_commit logic)
    """
    pr_map = {pr['number']: pr for pr in prs}
    final_chains = []

    for chain_pr_numbers in tqdm(chains, desc="Processing and enriching chains"):
        chain_prs = [pr_map[num] for num in chain_pr_numbers if num in pr_map]
        chain_prs.sort(key=lambda pr: datetime.fromisoformat(pr['created_at'].replace('Z', '+00:00')))

        repo_name_sanitized = repo_obj.repo.full_name.replace("/", "__")
        chain_id = f"{repo_name_sanitized}_chain_{chain_prs[0]['number']}"
        
        chain_object = {
            "chain_id": chain_id,
            "chain_length": len(chain_prs),
            "repository": repo_obj.repo.full_name,
            "turns": []
        }

        is_chain_valid = True
        for i, pr in enumerate(chain_prs):
            task_instance = create_instance_with_git(pr, repo_obj)
            
            if task_instance is None:
                is_chain_valid = False
                break

            # The base_commit for turn `i` is the merge_commit of turn `i-1`.
            if i > 0:
                prev_pr = chain_prs[i - 1]
                task_instance['base_commit'] = prev_pr['merge_commit_sha']
            
            task_instance['turn_id'] = i
            task_instance['depends_on'] = [chain_prs[j]['number'] for j in range(i)]
            chain_object['turns'].append(task_instance)
        
        if is_chain_valid and len(chain_object['turns']) > 1:
            final_chains.append(chain_object)
    
    return final_chains


def score_chain_quality(chain_prs: list[dict]) -> float:
    """
    Scores the quality of a potential chain based on several heuristics.

    Args:
        chain_prs (list[dict]): A chronologically sorted list of PR objects in the chain.

    Returns:
        float: A quality score for the chain. Higher is better.
    """
    score = 0.0
    
    # Heuristic 1: Chain Length (longer chains are more valuable)
    score += len(chain_prs) * 1.0

    # Heuristic 2: All PRs must be merged. This is a critical requirement.
    if not all(pr['merged_at'] for pr in chain_prs):
        return 0.0  # Invalid chain if any PR is not merged
    
    # Heuristic 3: Time between turns. Shorter time is a stronger signal.
    total_days = 0
    for i in range(len(chain_prs) - 1):
        date1 = datetime.fromisoformat(chain_prs[i]['created_at'].replace('Z', '+00:00'))
        date2 = datetime.fromisoformat(chain_prs[i+1]['created_at'].replace('Z', '+00:00'))
        total_days += (date2 - date1).days
    
    avg_days_between_turns = total_days / (len(chain_prs) - 1) if len(chain_prs) > 1 else 0
    
    # Penalize chains with long gaps
    if avg_days_between_turns < 7:
        score += 2.0
    elif avg_days_between_turns < 30:
        score += 1.0

    # Heuristic 4: All PRs from the same author is a strong signal.
    authors = {pr['user']['login'] for pr in chain_prs}
    if len(authors) == 1:
        score += 3.0
        
    return score


def create_instance_with_git(pr: dict, repo_obj: Repo) -> dict:
    """
    Creates a task instance from a PR by cloning the repo, generating the
    patch using git, and dynamically determining the software version.

    Returns a dictionary for the instance, or None if validation fails.
    """
    instance_id = f"{repo_obj.repo.full_name.replace('/', '__')}-{pr['number']}"

    # 1. Extract problem statement
    problem_statement, _ = extract_problem_statement_and_hints(pr, repo_obj)
    if not problem_statement:
        return None

    # 2. Generate patch using a temporary git clone
    try:
        with TemporaryDirectory() as temp_dir:
            repo_path = os.path.join(temp_dir, "repo")
            repo_url = f"https://github.com/{repo_obj.repo.full_name}.git"
            
            clone_result = subprocess.run(
                ["git", "clone", "--bare", repo_url, repo_path], 
                check=False, capture_output=True, text=True, timeout=300
            )
            if clone_result.returncode != 0:
                print(f"DEBUG: Skipping PR #{pr['number']}: Failed to clone repo. Error: {clone_result.stderr}")
                return None

            patch_cmd = ["git", "diff", pr["base"]["sha"], pr["merge_commit_sha"]]
            diff_result = subprocess.run(
                patch_cmd, cwd=repo_path, check=False, capture_output=True, text=True, timeout=60
            )
            if diff_result.returncode != 0:
                print(f"DEBUG: Skipping PR #{pr['number']}: Git diff failed. Error: {diff_result.stderr}")
                return None
            
            full_patch = diff_result.stdout
            if not full_patch:
                return None
    except Exception as e:
        print(f"DEBUG: Skipping PR #{pr['number']}: Subprocess failed. Error: {e}")
        return None
    
    # 3. Split patch into code and test patches
    patch_fix, patch_test = "", ""
    for hunk in PatchSet(full_patch):
        if any(test_word in hunk.path for test_word in ["test", "tests"]):
            patch_test += str(hunk) + "\n"
        else:
            patch_fix += str(hunk) + "\n"
    
    if not patch_fix.strip():
        return None

    # 4. Dynamically determine the version
    # Create a temporary instance dict for the get_version function
    temp_instance_for_versioning = {
        "repo": repo_obj.repo.full_name,
        "base_commit": pr["base"]["sha"]
    }
    version = get_version(temp_instance_for_versioning)
    if version is None:
        print(f"DEBUG: Skipping PR #{pr['number']}: Could not determine version.")
        return None

    # 5. Construct the final instance object
    instance = {
        "repo": repo_obj.repo.full_name,
        "pull_number": pr["number"],
        "instance_id": instance_id,
        "base_commit": pr["base"]["sha"],
        "merge_commit_sha": pr["merge_commit_sha"],
        "problem_statement": problem_statement,
        "patch": patch_fix.strip(),
        "test_patch": patch_test.strip(),
        "created_at": pr["created_at"],
        "version": version,  # Now dynamically determined
        "resolved_issues": pr.get("resolved_issues", [])
    }
    return instance