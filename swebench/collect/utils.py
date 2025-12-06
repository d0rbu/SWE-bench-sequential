from __future__ import annotations


import logging
import re
import requests
import time
import os

from bs4 import BeautifulSoup
from ghapi.core import GhApi
from fastcore.net import HTTP404NotFoundError, HTTP403ForbiddenError
from typing import Callable, Iterator, Optional
from unidiff import PatchSet

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests
PR_KEYWORDS = {
    "close",
    "closes",
    "closed",
    "fix",
    "fixes",
    "fixed",
    "resolve",
    "resolves",
    "resolved",
}


class Repo:
    def __init__(self, owner: str, name: str, token: Optional[str] = None):
        """
        Init to retrieve target repository and create ghapi tool

        Args:
            owner (str): owner of target repository
            name (str): name of target repository
            token (str): github token
        """
        self.owner = owner
        self.name = name
        self.token = token
        self.api = GhApi(token=token)
        self.repo = self.call_api(self.api.repos.get, owner=owner, repo=name)

    def call_api(self, func: Callable, **kwargs) -> dict | None:
        """
        API call wrapper with rate limit handling (checks every 5 minutes if rate limit is reset)

        Args:
            func (callable): API function to call
            **kwargs: keyword arguments to pass to API function
        Return:
            values (dict): response object of `func`
        """
        while True:
            try:
                values = func(**kwargs)
                return values
            except HTTP403ForbiddenError:
                while True:
                    rl = self.api.rate_limit.get()
                    logger.info(
                        f"[{self.owner}/{self.name}] Rate limit exceeded for token {self.token[:10]}, "
                        f"waiting for 5 minutes, remaining calls: {rl.resources.core.remaining}"
                    )
                    if rl.resources.core.remaining > 0:
                        break
                    time.sleep(60 * 5)
            except HTTP404NotFoundError:
                logger.info(f"[{self.owner}/{self.name}] Resource not found {kwargs}")
                return None

    def extract_resolved_issues(self, pull: dict) -> list[str]:
        """
        Extract list of issues referenced by a PR

        Args:
            pull (dict): PR dictionary object from GitHub
        Return:
            resolved_issues (list): list of issue numbers referenced by PR
        """
        # Define 1. issue number regex pattern 2. comment regex pattern 3. keywords
        issues_pat = re.compile(r"(\w+)\s+\#(\d+)")
        comments_pat = re.compile(r"(?s)<!--.*?-->")

        # Construct text to search over for issue numbers from PR body and commit messages
        text = pull.title if pull.title else ""
        text += "\n" + (pull.body if pull.body else "")
        commits = self.get_all_loop(
            self.api.pulls.list_commits, pull_number=pull.number, quiet=True
        )
        commit_messages = [commit.commit.message for commit in commits]
        commit_text = "\n".join(commit_messages) if commit_messages else ""
        text += "\n" + commit_text
        # Remove comments from text
        text = comments_pat.sub("", text)
        # Look for issue numbers in text via scraping <keyword, number> patterns
        references = issues_pat.findall(text)
        resolved_issues_set = set()
        if references:
            for word, issue_num in references:
                if word.lower() in PR_KEYWORDS:
                    resolved_issues_set.add(issue_num)
        return list(resolved_issues_set)

    def get_all_loop(
        self,
        func: Callable,
        per_page: int = 100,
        num_pages: Optional[int] = None,
        start_page: int = 1,
        quiet: bool = False,
        **kwargs,
    ) -> Iterator:
        """
        Return all values from a paginated API endpoint.

        Args:
            func (callable): API function to call
            per_page (int): number of values to return per page
            num_pages (int): number of pages to return
            start_page (int): page number to start from (default: 1)
            quiet (bool): whether to print progress
            **kwargs: keyword arguments to pass to API function
        """
        page = start_page
        args = {
            "owner": self.owner,
            "repo": self.name,
            "per_page": per_page,
            **kwargs,
        }
        while True:
            try:
                # Get values from API call
                values = func(**args, page=page)
                yield from values
                if len(values) == 0:
                    break
                if not quiet:
                    rl = self.api.rate_limit.get()
                    logger.info(
                        f"[{self.owner}/{self.name}] Processed page {page} ({per_page} values per page). "
                        f"Remaining calls: {rl.resources.core.remaining}"
                    )
                if num_pages is not None and page >= num_pages:
                    break
                page += 1
            except Exception as e:
                # Rate limit handling
                logger.error(
                    f"[{self.owner}/{self.name}] Error processing page {page} "
                    f"w/ token {self.token[:10]} - {e}"
                )
                while True:
                    rl = self.api.rate_limit.get()
                    if rl.resources.core.remaining > 0:
                        break
                    logger.info(
                        f"[{self.owner}/{self.name}] Waiting for rate limit reset "
                        f"for token {self.token[:10]}, checking again in 5 minutes"
                    )
                    time.sleep(60 * 5)
        if not quiet:
            logger.info(
                f"[{self.owner}/{self.name}] Processed {(page - start_page) * per_page + len(values)} values"
            )

    def get_all_issues(
        self,
        per_page: int = 100,
        num_pages: Optional[int] = None,
        direction: str = "desc",
        sort: str = "created",
        state: str = "closed",
        quiet: bool = False,
    ) -> Iterator:
        """
        Wrapper for API call to get all issues from repo

        Args:
            per_page (int): number of issues to return per page
            num_pages (int): number of pages to return
            direction (str): direction to sort issues
            sort (str): field to sort issues by
            state (str): state of issues to look for
            quiet (bool): whether to print progress
        """
        issues = self.get_all_loop(
            self.api.issues.list_for_repo,
            num_pages=num_pages,
            per_page=per_page,
            direction=direction,
            sort=sort,
            state=state,
            quiet=quiet,
        )
        return issues

    def get_all_pulls(
        self,
        per_page: int = 100,
        num_pages: Optional[int] = None,
        start_page: int = 1,
        direction: str = "desc",
        sort: str = "created",
        state: str = "closed",
        quiet: bool = False,
    ) -> Iterator:
        """
        Wrapper for API call to get all PRs from repo

        Args:
            per_page (int): number of PRs to return per page
            num_pages (int): number of pages to return
            start_page (int): page number to start from (default: 1)
            direction (str): direction to sort PRs
            sort (str): field to sort PRs by
            state (str): state of PRs to look for
            quiet (bool): whether to print progress
        """
        pulls = self.get_all_loop(
            self.api.pulls.list,
            num_pages=num_pages,
            start_page=start_page,
            direction=direction,
            per_page=per_page,
            sort=sort,
            state=state,
            quiet=quiet,
        )
        return pulls


def extract_problem_statement_and_hints(pull: dict, repo: Repo) -> tuple[str, str]:
    """
    Extract problem statement from issues associated with a pull request

    Args:
        pull (dict): PR dictionary object from GitHub
        repo (Repo): Repo object
    Return:
        text (str): problem statement
        hints (str): hints
    """
    if repo.name == "django":
        return extract_problem_statement_and_hints_django(pull, repo)
    text = ""
    all_hint_texts = list()
    for issue_number in pull["resolved_issues"]:
        issue = repo.call_api(
            repo.api.issues.get,
            owner=repo.owner,
            repo=repo.name,
            issue_number=issue_number,
        )
        if issue is None:
            continue
        title = issue.title if issue.title else ""
        body = issue.body if issue.body else ""
        text += f"{title}\n{body}\n"
        issue_number = issue.number
        hint_texts = _extract_hints(pull, repo, issue_number)
        hint_text = "\n".join(hint_texts)
        all_hint_texts.append(hint_text)
    return text, "\n".join(all_hint_texts) if all_hint_texts else ""


def extract_problem_statement_and_hints(pull: dict, repo: Repo) -> tuple[str, str]:
    """
    Extract problem statement from issues associated with a PR.
    If no issues are linked, fall back to using the PR's title and body.
    """
    text = ""
    all_hint_texts = list()
    
    # Primary Method: Use linked issues
    if pull.get("resolved_issues"):
        for issue_number in pull["resolved_issues"]:
            issue = repo.call_api(
                repo.api.issues.get,
                owner=repo.owner,
                repo=repo.name,
                issue_number=issue_number,
            )
            if issue is None:
                continue
            
            title = issue.title if issue.title else ""
            body = issue.body if issue.body else ""
            text += f"{title}\n{body}\n"
            
            # (Did not implemented hint extraction for PR-based problem statements for now)
    
    # Fallback Method: Use the PR's own title and body
    if not text.strip():
        title = pull.get("title", "") if pull.get("title") else ""
        body = pull.get("body", "") if pull.get("body") else ""
        text = f"{title}\n{body}\n".strip()

    # For now, not extracted hints from the PR body itself, so hints remain empty on fallback.
    return text, ""


def _extract_hints(pull: dict, repo: Repo, issue_number: int) -> list[str]:
    """
    Extract hints from comments associated with a pull request (before first commit)

    Args:
        pull (dict): PR dictionary object from GitHub
        repo (Repo): Repo object
        issue_number (int): issue number
    Return:
        hints (list): list of hints
    """
    # Get all commits in PR
    commits = repo.get_all_loop(
        repo.api.pulls.list_commits, pull_number=pull["number"], quiet=True
    )
    commits = list(commits)
    if len(commits) == 0:
        # If there are no comments, return no hints
        return []
    # Get time of first commit in PR
    commit_time = commits[0].commit.author.date  # str
    commit_time = time.mktime(time.strptime(commit_time, "%Y-%m-%dT%H:%M:%SZ"))
    # Get all comments in PR
    all_comments = repo.get_all_loop(
        repo.api.issues.list_comments, issue_number=issue_number, quiet=True
    )
    all_comments = list(all_comments)
    # Iterate through all comments, only keep comments created before first commit
    comments = list()
    for comment in all_comments:
        comment_time = time.mktime(
            time.strptime(comment.updated_at, "%Y-%m-%dT%H:%M:%SZ")
        )  # use updated_at instead of created_at
        if comment_time < commit_time:
            comments.append(comment)
        else:
            break
        # only include information available before the first commit was created
    # Keep text from comments
    comments = [comment.body for comment in comments]
    return comments


def extract_modified_files(pull: dict, repo: Repo) -> list[str]:
    """
    Extract the list of modified files from a pull request using the GitHub API.

    Args:
        pull (dict): The pull request object from the GitHub API.
        repo (Repo): The repository object containing the ghapi client.
    
    Returns:
        list[str]: A list of file paths that were modified in the pull request.
    """
    try:
        # Using the 'list_files' endpoint
        files = repo.call_api(
            repo.api.pulls.list_files,
            owner=repo.owner,
            repo=repo.name,
            pull_number=pull['number']
        )
        if files:
            return [f.filename for f in files]
        return []
    except Exception as e:
        logger.error(f"Failed to extract modified files for PR #{pull['number']}: {e}")
        return []


def extract_patches(pull: dict, repo: Repo) -> tuple[str, str]:
    """
    Get patch and test patch from PR

    Args:
        pull (dict): PR dictionary object from GitHub
        repo (Repo): Repo object
    Return:
        patch_change_str (str): gold patch
        patch_test_str (str): test patch
    """
    patch = requests.get(pull["diff_url"]).text
    patch_test = ""
    patch_fix = ""
    for hunk in PatchSet(patch):
        if any(
            test_word in hunk.path for test_word in ["test", "tests", "e2e", "testing"]
        ):
            patch_test += str(hunk)
        else:
            patch_fix += str(hunk)
    return patch_fix, patch_test


### MARK: Repo Specific Parsing Functions ###
def extract_problem_statement_and_hints_django(
    pull: dict, repo: Repo
) -> tuple[str, list[str]]:
    """
    Get problem statement and hints from issues associated with a pull request

    Args:
        pull (dict): PR dictionary object from GitHub
        repo (Repo): Repo object
    Return:
        text (str): problem statement
        hints (str): hints
    """
    text = ""
    all_hints_text = list()
    for issue_number in pull["resolved_issues"]:
        url = f"https://code.djangoproject.com/ticket/{issue_number}"
        resp = requests.get(url)
        if resp.status_code != 200:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")

        # Get problem statement (title + body)
        issue_desc = soup.find("div", {"id": "ticket"})
        title = issue_desc.find("h1", class_="searchable").get_text()
        title = re.sub(r"\s+", " ", title).strip()
        body = issue_desc.find("div", class_="description").get_text()
        body = re.sub(r"\n+", "\n", body)
        body = re.sub(r"    ", "\t", body)
        body = re.sub(r"[ ]{2,}", " ", body).strip()
        text += f"{title}\n{body}\n"

        # Get time of first commit in PR
        commits = repo.get_all_loop(
            repo.api.pulls.list_commits, pull_number=pull["number"], quiet=True
        )
        commits = list(commits)
        if len(commits) == 0:
            continue
        commit_time = commits[0].commit.author.date
        commit_time = time.mktime(time.strptime(commit_time, "%Y-%m-%dT%H:%M:%SZ"))

        # Get all comments before first commit
        comments_html = soup.find("div", {"id": "changelog"})
        div_blocks = comments_html.find_all("div", class_="change")
        # Loop through each div block
        for div_block in div_blocks:
            # Find the comment text and timestamp
            comment_resp = div_block.find("div", class_="comment")
            timestamp_resp = div_block.find("a", class_="timeline")
            if comment_resp is None or timestamp_resp is None:
                continue

            comment_text = re.sub(r"\s+", " ", comment_resp.text).strip()
            timestamp = timestamp_resp["title"]
            if timestamp.startswith("See timeline at "):
                timestamp = timestamp[len("See timeline at ") :]
            if "/" in timestamp:
                timestamp = time.mktime(time.strptime(timestamp, "%m/%d/%y %H:%M:%S"))
            elif "," in timestamp:
                timestamp = time.mktime(
                    time.strptime(timestamp, "%b %d, %Y, %I:%M:%S %p")
                )
            else:
                raise ValueError(f"Timestamp format not recognized: {timestamp}")

            # Append the comment and timestamp as a tuple to the comments list
            if timestamp < commit_time:
                all_hints_text.append((comment_text, timestamp))

    return text, all_hints_text


def build_dependency_graph(prs_with_files: list[dict]) -> dict[int, set[int]]:
    """
    Builds a dependency graph from PRs using file overlap, issue references,
    and temporal proximity. (WITH DIAGNOSTIC LOGGING)
    """
    adj_list = {pr['number']: set() for pr in prs_with_files}
    
    # --- Counters for our signals ---
    edges_from_files = 0
    edges_from_issues = 0
    edges_from_temporal = 0

    # --- Signal 1: File Overlap ---
    file_to_prs = {}
    for pr in prs_with_files:
        for file_path in pr.get('modified_files', []):
            if file_path not in file_to_prs:
                file_to_prs[file_path] = []
            file_to_prs[file_path].append(pr['number'])

    for file_path, pr_numbers in file_to_prs.items():
        if len(pr_numbers) > 1:
            for i in range(len(pr_numbers)):
                for j in range(i + 1, len(pr_numbers)):
                    if pr_numbers[j] not in adj_list[pr_numbers[i]]:
                        adj_list[pr_numbers[i]].add(pr_numbers[j])
                        adj_list[pr_numbers[j]].add(pr_numbers[i])
                        edges_from_files += 1

    # --- Signal 2: Shared Issue References ---
    issue_to_prs = {}
    for pr in prs_with_files:
        for issue_num in pr.get('resolved_issues', []):
            if issue_num not in issue_to_prs:
                issue_to_prs[issue_num] = []
            issue_to_prs[issue_num].append(pr['number'])

    for issue_num, pr_numbers in issue_to_prs.items():
        if len(pr_numbers) > 1:
            for i in range(len(pr_numbers)):
                for j in range(i + 1, len(pr_numbers)):
                    if pr_numbers[j] not in adj_list[pr_numbers[i]]:
                        adj_list[pr_numbers[i]].add(pr_numbers[j])
                        adj_list[pr_numbers[j]].add(pr_numbers[i])
                        edges_from_issues += 1

    # --- Signal 3: Temporal & Directory Proximity by Same Author ---
    from datetime import datetime, timedelta
    
    author_prs = {}
    for pr in prs_with_files:
        author = pr.get('user', {}).get('login')
        if not author:
            continue
        if author not in author_prs:
            author_prs[author] = []
        
        created_at = datetime.fromisoformat(pr['created_at'].replace('Z', '+00:00'))
        dirs = {os.path.dirname(f) for f in pr.get('modified_files', []) if f}
        author_prs[author].append({'number': pr['number'], 'created_at': created_at, 'dirs': dirs})

    for author, pr_list in author_prs.items():
        if len(pr_list) > 1:
            pr_list.sort(key=lambda p: p['created_at'])
            for i in range(len(pr_list)):
                for j in range(i + 1, len(pr_list)):
                    pr1 = pr_list[i]
                    pr2 = pr_list[j]
                    if pr2['created_at'] - pr1['created_at'] < timedelta(days=7):
                        if pr1['dirs'].intersection(pr2['dirs']):
                            if pr2['number'] not in adj_list[pr1['number']]:
                                adj_list[pr1['number']].add(pr2['number'])
                                adj_list[pr2['number']].add(pr1['number'])
                                edges_from_temporal += 1
                    else:
                        break
    
    # --- Final Diagnostic Printout ---
    print("\n--- Dependency Graph Analysis ---")
    print(f"Edges found from file overlap: {edges_from_files}")
    print(f"Edges found from shared issues: {edges_from_issues}")
    print(f"Edges found from temporal proximity: {edges_from_temporal}")
    total_edges = sum(len(v) for v in adj_list.values()) // 2
    print(f"Total unique edges in graph: {total_edges}")
    print("---------------------------------\n")

    return adj_list


def find_connected_components(adj_list: dict[int, set[int]]) -> list[list[int]]:
    """
    Finds all connected components in a graph using Breadth-First Search (BFS).

    Args:
        adj_list (dict[int, set[int]]): The adjacency list of the graph.

    Returns:
        list[list[int]]: A list of lists, where each inner list contains the
                         PR numbers of a connected component (a chain).
    """
    visited = set()
    chains = []
    for pr_number in adj_list:
        if pr_number not in visited:
            chain = []
            queue = [pr_number]
            visited.add(pr_number)
            while queue:
                current_pr = queue.pop(0)
                chain.append(current_pr)
                for neighbor in adj_list[current_pr]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)
            chains.append(sorted(chain))
    return chains