#!/usr/bin/env python3
"""
Multi-turn inference script for SWE-bench chain evaluation.

This script generates predictions for multi-turn chains where:
- Turn 0: User presents the initial issue, model generates a patch
- Turn N: User presents the next issue (from the chain), model generates the next patch

The conversation history accumulates across turns, simulating a multi-turn code editing session.
"""

import json
import logging
import os
import traceback
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

import dotenv
import openai
from anthropic import Anthropic
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
from tqdm.auto import tqdm

from swebench.harness.constants import KEY_INSTANCE_ID, KEY_MODEL, KEY_PREDICTION
from swebench.inference.make_datasets.utils import extract_diff

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
dotenv.load_dotenv()

# Model limits for context window
MODEL_LIMITS = {
    "claude-instant-1": 100_000,
    "claude-2": 100_000,
    "claude-3-opus-20240229": 200_000,
    "claude-3-5-sonnet-20241022": 200_000,
    "claude-3-sonnet-20240229": 200_000,
    "claude-3-haiku-20240307": 200_000,
    "claude-sonnet-4-5-20250929": 200_000,
    "claude-haiku-4-5-20251001": 200_000,
    "claude-opus-4-5-20251101": 200_000,
    "gpt-3.5-turbo-16k-0613": 16_385,
    "gpt-3.5-turbo-0613": 4_097,
    "gpt-3.5-turbo-1106": 16_385,
    "gpt-4-32k-0613": 32_768,
    "gpt-4-0613": 8_192,
    "gpt-4-1106-preview": 128_000,
    "gpt-4-0125-preview": 128_000,
    "gpt-4o": 128_000,
    "gpt-4o-mini": 128_000,
    "gpt-5": 400_000,
    "gpt-5-mini": 400_000,
    "gpt-5-nano": 400_000,
    "gpt-5.1": 400_000,
    "gpt-5.1-codex": 400_000,
}

# Cost per token for each model
MODEL_COST_PER_INPUT = {
    "claude-instant-1": 0.00000163,
    "claude-2": 0.00001102,
    "claude-3-opus-20240229": 0.000015,
    "claude-3-5-sonnet-20241022": 0.000003,
    "claude-3-sonnet-20240229": 0.000003,
    "claude-3-haiku-20240307": 0.00000025,
    "gpt-3.5-turbo-16k-0613": 0.0000015,
    "gpt-3.5-turbo-0613": 0.0000015,
    "gpt-3.5-turbo-1106": 0.000001,
    "claude-sonnet-4-5-20250929": 0.000003,
    "claude-haiku-4-5-20251001": 0.000001,
    "claude-opus-4-5-20251101": 0.000005,
    "gpt-4-0613": 0.00003,
    "gpt-4-32k-0613": 0.00006,
    "gpt-4-1106-preview": 0.00001,
    "gpt-4-0125-preview": 0.00001,
    "gpt-4o": 0.000005,
    "gpt-4o-mini": 0.00000015,
    "gpt-5": 0.00000125,
    "gpt-5-mini": 0.00000025,
    "gpt-5-nano": 0.00000005,
    "gpt-5.1": 0.00000125,
    "gpt-5.1-codex": 0.00000125,
}

MODEL_COST_PER_OUTPUT = {
    "claude-instant-1": 0.00000551,
    "claude-2": 0.00003268,
    "claude-3-opus-20240229": 0.000075,
    "claude-3-5-sonnet-20241022": 0.000015,
    "claude-3-sonnet-20240229": 0.000015,
    "claude-3-haiku-20240307": 0.00000125,
    "claude-sonnet-4-5-20250929": 0.000015,
    "claude-haiku-4-5-20251001": 0.000005,
    "claude-opus-4-5-20251101": 0.000025,
    "gpt-3.5-turbo-16k-0613": 0.000002,
    "gpt-3.5-turbo-16k": 0.000002,
    "gpt-3.5-turbo-1106": 0.000002,
    "gpt-4-0613": 0.00006,
    "gpt-4-32k-0613": 0.00012,
    "gpt-4-1106-preview": 0.00003,
    "gpt-4-0125-preview": 0.00003,
    "gpt-4o": 0.000015,
    "gpt-4o-mini": 0.0000006,
    "gpt-5": 0.00001,
    "gpt-5-mini": 0.000002,
    "gpt-5-nano": 0.0000004,
    "gpt-5.1": 0.00001,
    "gpt-5.1-codex": 0.00001,
}


SYSTEM_PROMPT = """You are a software engineer working on fixing issues in a codebase.
When presented with an issue, analyze it carefully and generate a patch file that fixes the issue.
Your response should include a patch in the standard unified diff format that can be applied using `git apply`.

Format your patch like this:
```diff
--- a/path/to/file.py
+++ b/path/to/file.py
@@ -line_num,count +line_num,count @@
 context line
-removed line
+added line
 context line
```

Only output the patch, no additional explanation is needed unless you cannot generate a fix."""


PATCH_EXAMPLE = """--- a/file.py
+++ b/file.py
@@ -1,5 +1,5 @@
 def example():
-    return "old"
+    return "new"
"""


def calc_cost(model_name: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate the cost of an API call."""
    input_cost = MODEL_COST_PER_INPUT.get(model_name, 0) * input_tokens
    output_cost = MODEL_COST_PER_OUTPUT.get(model_name, 0) * output_tokens
    return input_cost + output_cost


def load_chains(chains_path: str) -> list:
    """Load chains from a JSON/JSONL file."""
    with open(chains_path, "r") as f:
        content = f.read().strip()

    chains = []
    decoder = json.JSONDecoder()
    pos = 0
    while pos < len(content):
        try:
            obj, pos = decoder.raw_decode(content, pos)
            chains.append(obj)
            while pos < len(content) and content[pos].isspace():
                pos += 1
        except json.JSONDecodeError:
            break

    return chains


def format_issue_message(turn: dict, turn_idx: int, is_first_turn: bool) -> str:
    """Format an issue as a user message for the model."""
    problem_statement = turn.get("problem_statement", "")
    instance_id = turn.get("instance_id", f"turn_{turn_idx}")

    if is_first_turn:
        return f"""Please fix the following issue in the codebase.

<issue>
Instance ID: {instance_id}

{problem_statement}
</issue>

Generate a patch file that fixes this issue. Use the standard unified diff format."""
    else:
        return f"""Great! Now please address the following additional issue that needs to be fixed.
Note: Your previous patches have been applied to the codebase.

<issue>
Instance ID: {instance_id}

{problem_statement}
</issue>

Generate a patch file that fixes this new issue, building on your previous changes."""


@retry(wait=wait_random_exponential(min=30, max=600), stop=stop_after_attempt(3))
def call_openai(
    messages: list,
    model_name: str,
    temperature: float = 0.2,
    top_p: float = 0.95,
    max_tokens: int = 4096,
) -> tuple:
    """Call OpenAI API with retry logic."""
    response = openai.chat.completions.create(
        model=model_name,
        messages=messages,
        temperature=temperature,
        top_p=top_p,
        max_tokens=max_tokens,
    )

    input_tokens = response.usage.prompt_tokens
    output_tokens = response.usage.completion_tokens
    cost = calc_cost(model_name, input_tokens, output_tokens)
    content = response.choices[0].message.content

    return content, cost


@retry(wait=wait_random_exponential(min=60, max=600), stop=stop_after_attempt(6))
def call_anthropic(
    messages: list,
    anthropic_client: Anthropic,
    model_name: str,
    temperature: float = 0.2,
    top_p: float = 0.95,
    max_tokens: int = 4096,
    system_prompt: str = SYSTEM_PROMPT,
) -> tuple:
    """Call Anthropic API with retry logic."""
    response = anthropic_client.messages.create(
        model=model_name,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
        top_p=top_p,
        system=system_prompt,
    )

    input_tokens = response.usage.input_tokens
    output_tokens = response.usage.output_tokens
    cost = calc_cost(model_name, input_tokens, output_tokens)
    content = response.content[0].text

    return content, cost


def run_chain_inference(
    chain: dict,
    model_name: str,
    api_client,
    temperature: float = 0.2,
    top_p: float = 0.95,
    max_tokens: int = 4096,
) -> dict:
    """
    Run multi-turn inference for a single chain.

    Args:
        chain: Chain dictionary with task_instances
        model_name: Name of the model to use
        api_client: API client (OpenAI client or Anthropic client)
        temperature: Sampling temperature
        top_p: Top-p sampling parameter
        max_tokens: Maximum tokens per response

    Returns:
        Dictionary with chain_id, model_name_or_path, and turn_predictions
    """
    chain_id = chain["chain_id"]
    task_instances = chain["task_instances"]

    # Initialize conversation history
    messages = []
    turn_predictions = []
    total_cost = 0

    is_openai = model_name.startswith("gpt")

    for turn_idx, turn in enumerate(task_instances):
        instance_id = turn["instance_id"]
        is_first_turn = turn_idx == 0

        # Format the user message for this turn
        user_message = format_issue_message(turn, turn_idx, is_first_turn)

        # Add user message to history
        messages.append({"role": "user", "content": user_message})

        try:
            if is_openai:
                # OpenAI format includes system message in the messages list
                full_messages = [
                    {"role": "system", "content": SYSTEM_PROMPT}
                ] + messages
                response_content, cost = call_openai(
                    full_messages,
                    model_name,
                    temperature=temperature,
                    top_p=top_p,
                    max_tokens=max_tokens,
                )
            else:
                # Anthropic format uses system parameter separately
                response_content, cost = call_anthropic(
                    messages,
                    api_client,
                    model_name,
                    temperature=temperature,
                    top_p=top_p,
                    max_tokens=max_tokens,
                    system_prompt=SYSTEM_PROMPT,
                )

            total_cost += cost

            # Extract the patch from the response
            model_patch = extract_diff(response_content)

            # Add assistant response to history for next turn
            messages.append({"role": "assistant", "content": response_content})

            turn_predictions.append(
                {
                    KEY_INSTANCE_ID: instance_id,
                    KEY_PREDICTION: model_patch,
                    "full_output": response_content,
                    "turn_index": turn_idx,
                }
            )

            logger.info(
                f"  Turn {turn_idx} ({instance_id}): Generated patch ({cost:.4f}$)"
            )

        except Exception as e:
            logger.error(f"  Turn {turn_idx} ({instance_id}): Error - {e}")
            traceback.print_exc()

            # Record empty prediction for failed turn
            turn_predictions.append(
                {
                    KEY_INSTANCE_ID: instance_id,
                    KEY_PREDICTION: None,
                    "full_output": None,
                    "turn_index": turn_idx,
                    "error": str(e),
                }
            )

            # Add placeholder to maintain conversation continuity
            messages.append(
                {
                    "role": "assistant",
                    "content": "I apologize, but I was unable to generate a patch for this issue.",
                }
            )

    return {
        "chain_id": chain_id,
        KEY_MODEL: model_name,
        "turn_predictions": turn_predictions,
        "total_cost": total_cost,
        "num_turns": len(task_instances),
    }


def main(
    chains_path: str,
    model_name: str,
    output_file: str,
    temperature: float = 0.2,
    top_p: float = 0.95,
    max_tokens: int = 4096,
    max_cost: Optional[float] = None,
    chain_ids: Optional[list] = None,
):
    """
    Main function to run multi-turn inference on chains.

    Args:
        chains_path: Path to chains JSONL file
        model_name: Name of the model to use
        output_file: Path to output predictions file
        temperature: Sampling temperature
        top_p: Top-p sampling parameter
        max_tokens: Maximum tokens per response
        max_cost: Maximum total cost (stops when reached)
        chain_ids: Optional list of specific chain IDs to process
    """
    # Load chains
    logger.info(f"Loading chains from: {chains_path}")
    chains = load_chains(chains_path)
    logger.info(f"Loaded {len(chains)} chains")

    # Filter to specific chain IDs if provided
    if chain_ids:
        chains = [c for c in chains if c["chain_id"] in chain_ids]
        logger.info(f"Filtered to {len(chains)} chains")

    # Load existing predictions to enable resumption
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    existing_chain_ids = set()
    if output_path.exists():
        with open(output_path, "r") as f:
            for line in f:
                if line.strip():
                    pred = json.loads(line)
                    existing_chain_ids.add(pred["chain_id"])
        logger.info(f"Found {len(existing_chain_ids)} already processed chains")

    # Filter out already processed chains
    chains_to_process = [c for c in chains if c["chain_id"] not in existing_chain_ids]
    logger.info(f"Will process {len(chains_to_process)} chains")

    if not chains_to_process:
        logger.info("No chains to process. Exiting.")
        return

    # Setup API client
    is_openai = model_name.startswith("gpt")

    if is_openai:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        openai.api_key = api_key
        api_client = None  # OpenAI uses module-level client
        logger.info(
            f"Using OpenAI API with key {'*' * (len(api_key) - 5) + api_key[-5:]}"
        )
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
        api_client = Anthropic(api_key=api_key)
        logger.info(
            f"Using Anthropic API with key {'*' * (len(api_key) - 5) + api_key[-5:]}"
        )

    # Process chains
    total_cost = 0

    with open(output_path, "a") as f:
        for chain in tqdm(chains_to_process, desc=f"Inference ({model_name})"):
            chain_id = chain["chain_id"]
            logger.info(f"Processing chain: {chain_id}")

            try:
                result = run_chain_inference(
                    chain=chain,
                    model_name=model_name,
                    api_client=api_client,
                    temperature=temperature,
                    top_p=top_p,
                    max_tokens=max_tokens,
                )

                total_cost += result.get("total_cost", 0)

                # Write result to file
                f.write(json.dumps(result) + "\n")
                f.flush()

                logger.info(
                    f"Chain {chain_id}: Completed ({result['total_cost']:.4f}$, total: {total_cost:.4f}$)"
                )

            except Exception as e:
                logger.error(f"Chain {chain_id}: Failed - {e}")
                traceback.print_exc()

                # Write error result
                error_result = {
                    "chain_id": chain_id,
                    KEY_MODEL: model_name,
                    "turn_predictions": [],
                    "error": str(e),
                }
                f.write(json.dumps(error_result) + "\n")
                f.flush()

            # Check cost limit
            if max_cost is not None and total_cost >= max_cost:
                logger.info(f"Reached max cost limit ({max_cost}$). Stopping.")
                break

    logger.info(f"Done! Total cost: {total_cost:.4f}$")
    logger.info(f"Predictions saved to: {output_path}")


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--chains_path",
        type=str,
        required=True,
        help="Path to chains JSONL file",
    )
    parser.add_argument(
        "--model_name",
        type=str,
        required=True,
        help="Name of the model to use (e.g., gpt-4o, claude-3-5-sonnet-20241022)",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        required=True,
        help="Path to output predictions file (JSONL)",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.2,
        help="Sampling temperature",
    )
    parser.add_argument(
        "--top_p",
        type=float,
        default=0.95,
        help="Top-p sampling parameter",
    )
    parser.add_argument(
        "--max_tokens",
        type=int,
        default=4096,
        help="Maximum tokens per response",
    )
    parser.add_argument(
        "--max_cost",
        type=float,
        default=None,
        help="Maximum total cost (stops when reached)",
    )
    parser.add_argument(
        "--chain_ids",
        type=str,
        nargs="+",
        default=None,
        help="Optional list of specific chain IDs to process",
    )

    args = parser.parse_args()
    main(**vars(args))
