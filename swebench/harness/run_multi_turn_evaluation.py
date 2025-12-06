import json
import traceback
from argparse import ArgumentParser
from pathlib import Path

import docker
from tqdm import tqdm

from swebench.harness.constants import (
    KEY_INSTANCE_ID,
    KEY_MODEL,
    KEY_PREDICTION,
    LOG_TEST_OUTPUT,
)
from swebench.harness.docker_build import (
    build_container,
    build_env_images,
    close_logger,
    setup_logger,
)
from swebench.harness.docker_utils import (
    cleanup_container,
    copy_to_container,
    exec_run_with_timeout,
)
from swebench.harness.grading import compute_chain_metrics, get_eval_report
from swebench.harness.test_spec.test_spec import make_test_spec

# Define constants for multi-turn logging
MULTI_TURN_LOG_DIR = Path("logs/run_multi_turn_evaluation")


def load_chains_from_file(chains_path: str) -> list:
    """
    Load chains from a JSON/JSONL file.

    Args:
        chains_path: Path to chains file (JSON or JSONL)

    Returns:
        List of chain dictionaries
    """
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


def get_chain_predictions(
    chains: list,
    predictions_path: str,
) -> dict:
    """
    Get predictions for each chain.

    For predictions_path == "gold": Use patches from the chain task instances.
    For predictions_path pointing to a file: Load model predictions.

    Args:
        chains: List of chain dictionaries
        predictions_path: Either "gold" or path to predictions file

    Returns:
        Dictionary mapping chain_id -> list of turn predictions
        Each prediction has: instance_id, model_patch, model_name_or_path
    """
    if predictions_path == "gold":
        print("Using gold predictions from chain patches")
        predictions = {}
        for chain in chains:
            chain_id = chain["chain_id"]
            turn_predictions = []
            for turn in chain["task_instances"]:
                turn_predictions.append(
                    {
                        KEY_INSTANCE_ID: turn["instance_id"],
                        KEY_PREDICTION: turn["patch"],
                        KEY_MODEL: "gold",
                    }
                )
            predictions[chain_id] = turn_predictions
        return predictions

    # Load predictions from file
    print(f"Loading predictions from: {predictions_path}")

    if predictions_path.endswith(".json"):
        with open(predictions_path, "r") as f:
            raw_predictions = json.load(f)
    elif predictions_path.endswith(".jsonl"):
        with open(predictions_path, "r") as f:
            raw_predictions = [json.loads(line) for line in f if line.strip()]
    else:
        raise ValueError("Predictions path must be .json or .jsonl")

    # Convert to dictionary keyed by chain_id
    # Expected format for multi-turn predictions:
    # {
    #     "chain_id": "...",
    #     "model_name_or_path": "...",
    #     "turn_predictions": [
    #         {"instance_id": "...", "model_patch": "..."},
    #         ...
    #     ]
    # }
    predictions = {}

    if isinstance(raw_predictions, list):
        for pred in raw_predictions:
            if "chain_id" in pred:
                # Multi-turn chain format
                chain_id = pred["chain_id"]
                model_name = pred.get(KEY_MODEL, "unknown")
                turn_preds = []
                for turn_pred in pred.get("turn_predictions", []):
                    turn_preds.append(
                        {
                            KEY_INSTANCE_ID: turn_pred.get(
                                KEY_INSTANCE_ID, turn_pred.get("instance_id")
                            ),
                            KEY_PREDICTION: turn_pred.get(
                                KEY_PREDICTION, turn_pred.get("model_patch")
                            ),
                            KEY_MODEL: model_name,
                        }
                    )
                predictions[chain_id] = turn_preds
            else:
                raise ValueError(
                    "Prediction format must include 'chain_id' for multi-turn evaluation. "
                    "Expected format: {chain_id, model_name_or_path, turn_predictions: [{instance_id, model_patch}, ...]}"
                )
    elif isinstance(raw_predictions, dict):
        # Assume it's already keyed by chain_id
        for chain_id, chain_pred in raw_predictions.items():
            if isinstance(chain_pred, list):
                # List of turn predictions
                predictions[chain_id] = [
                    {
                        KEY_INSTANCE_ID: p.get(KEY_INSTANCE_ID, p.get("instance_id")),
                        KEY_PREDICTION: p.get(KEY_PREDICTION, p.get("model_patch")),
                        KEY_MODEL: p.get(KEY_MODEL, "unknown"),
                    }
                    for p in chain_pred
                ]
            elif isinstance(chain_pred, dict) and "turn_predictions" in chain_pred:
                model_name = chain_pred.get(KEY_MODEL, "unknown")
                predictions[chain_id] = [
                    {
                        KEY_INSTANCE_ID: p.get(KEY_INSTANCE_ID, p.get("instance_id")),
                        KEY_PREDICTION: p.get(KEY_PREDICTION, p.get("model_patch")),
                        KEY_MODEL: model_name,
                    }
                    for p in chain_pred["turn_predictions"]
                ]

    return predictions


def run_chain_in_container(
    chain: dict,
    chain_predictions: list,
    client: docker.DockerClient,
    run_id: str,
    timeout: int = 1800,
):
    """
    Runs a single multi-turn chain within a dedicated Docker container.

    Args:
        chain: Chain dictionary containing task_instances
        chain_predictions: List of predictions for each turn in the chain
        client: Docker client
        run_id: Unique identifier for this evaluation run
        timeout: Timeout for running tests (default 1800 seconds)
    """
    chain_id = chain["chain_id"]
    print(f"\n--- Running Chain: {chain_id} ---")

    # Setup logging for this specific chain
    model_name = (
        chain_predictions[0].get(KEY_MODEL, "unknown")
        if chain_predictions
        else "unknown"
    )
    chain_log_dir = (
        MULTI_TURN_LOG_DIR / run_id / model_name.replace("/", "__") / chain_id
    )
    chain_log_dir.mkdir(parents=True, exist_ok=True)
    logger = setup_logger(chain_id, chain_log_dir / "chain_run.log")

    container = None
    try:
        # 1. Use the FIRST turn to define the environment
        first_turn_instance = chain["task_instances"][0]
        test_spec = make_test_spec(first_turn_instance)

        # 2. Build and start the container
        logger.info("Building and starting container...")
        container = build_container(
            test_spec, client, run_id, logger, nocache=False, force_rebuild=False
        )
        container.start()
        logger.info(f"Container {container.id} started.")

        # Validate prediction count matches turn count
        num_turns = len(chain["task_instances"])
        num_predictions = len(chain_predictions)
        if num_predictions != num_turns:
            logger.warning(
                f"Prediction count ({num_predictions}) does not match turn count ({num_turns}). "
                f"Will process min({num_predictions}, {num_turns}) turns."
            )

        # 3. Loop through each turn
        turns_to_process = min(num_turns, num_predictions)
        for turn_idx in range(turns_to_process):
            turn = chain["task_instances"][turn_idx]
            prediction = chain_predictions[turn_idx]

            instance_id = turn["instance_id"]
            logger.info(f"--- Starting Turn {turn_idx} (Instance: {instance_id}) ---")

            # a. Apply the patch for the current turn
            patch_content = prediction.get(KEY_PREDICTION, "")

            if patch_content and not patch_content.endswith("\n"):
                patch_content += "\n"

            patch_path = Path(chain_log_dir / f"turn_{turn_idx}_patch.diff")
            patch_path.write_text(patch_content)

            # Handle empty patches
            if not patch_content.strip():
                logger.warning(
                    f"Turn {turn_idx}: Empty or missing patch, skipping patch application."
                )
                turn["evaluation_result"] = {
                    "resolved": False,
                    "patch_is_None": True,
                    "patch_exists": False,
                    "patch_successfully_applied": False,
                }
                continue

            container_patch_path_str = f"/tmp/turn_{turn_idx}.diff"
            container_patch_path_obj = Path(container_patch_path_str)
            copy_to_container(container, patch_path, container_patch_path_obj)

            # *** A robust, multi-command apply strategy ***
            apply_commands = [
                f"git apply --verbose {container_patch_path_str}",
                f"git apply --verbose --reject {container_patch_path_str}",
                f"patch --batch --fuzz=5 -p1 -i {container_patch_path_str}",
            ]

            applied_successfully = False
            for cmd in apply_commands:
                apply_result = container.exec_run(cmd, workdir="/testbed")
                if apply_result.exit_code == 0:
                    logger.info(
                        f"Turn {turn_idx}: Patch applied successfully with command: '{cmd}'"
                    )
                    logger.info(apply_result.output.decode("utf-8"))
                    applied_successfully = True
                    break
                else:
                    logger.warning(
                        f"Turn {turn_idx}: Command failed: '{cmd}'. Trying next command."
                    )
                    logger.warning(apply_result.output.decode("utf-8"))

            if not applied_successfully:
                logger.error(
                    f"Turn {turn_idx}: All patch application methods FAILED. Recording failure and continuing."
                )
                turn["evaluation_result"] = {
                    "resolved": False,
                    "patch_is_None": False,
                    "patch_exists": True,
                    "patch_successfully_applied": False,
                }
                # Continue to next turn instead of aborting entire chain
                continue

            # b. Prepare and Run the Test Script
            logger.info(f"Turn {turn_idx}: Generating evaluation script...")

            # Create a TestSpec for this specific turn to generate the correct eval script
            current_turn_spec = make_test_spec(turn)

            # Write the script to a local file first
            eval_script_content = current_turn_spec.eval_script
            eval_script_path = Path(chain_log_dir / f"turn_{turn_idx}_eval.sh")
            eval_script_path.write_text(eval_script_content)

            # Copy it into the container
            container_eval_path = Path("/eval.sh")
            copy_to_container(container, eval_script_path, container_eval_path)

            # Execute the script inside the container
            logger.info(f"Turn {turn_idx}: Running tests (timeout={timeout}s)...")
            try:
                test_output, timed_out, total_runtime = exec_run_with_timeout(
                    container, "/bin/bash /eval.sh", timeout=timeout
                )
            except Exception as e:
                logger.error(f"Turn {turn_idx}: Test execution failed with error: {e}")
                test_output = f"EXECUTION ERROR: {str(e)}"
                timed_out = False

            # Save the raw output logs
            test_output_file = chain_log_dir / f"turn_{turn_idx}_{LOG_TEST_OUTPUT}"
            test_output_file.write_text(test_output)

            if timed_out:
                logger.warning(f"Turn {turn_idx}: Tests timed out.")

            # c. Grade the result
            logger.info(f"Turn {turn_idx}: Grading results...")

            # Construct prediction object for get_eval_report
            prediction_wrapper = {
                KEY_INSTANCE_ID: turn["instance_id"],
                KEY_PREDICTION: patch_content,
                KEY_MODEL: prediction.get(KEY_MODEL, "unknown"),
            }

            # Parse the logs to determine Pass/Fail
            report = get_eval_report(
                test_spec=current_turn_spec,
                prediction=prediction_wrapper,
                test_log_path=str(test_output_file),
                include_tests_status=True,
            )

            # Log the result
            is_resolved = report[turn["instance_id"]]["resolved"]
            logger.info(f"Turn {turn_idx} Result: Resolved={is_resolved}")

            # Store result in the turn object for metrics calculation later
            turn["evaluation_result"] = report[turn["instance_id"]]

            # d. Commit the changes to persist the state for the next turn
            commit_msg = f"SWE-bench multi-turn: Apply fix for {instance_id}"
            container.exec_run(
                'git config --global user.email "swe-bench@example.com"',
                workdir="/testbed",
            )
            container.exec_run(
                'git config --global user.name "SWE-bench Runner"', workdir="/testbed"
            )
            container.exec_run("git add .", workdir="/testbed")
            container.exec_run(["git", "commit", "-m", commit_msg], workdir="/testbed")
            logger.info(f"Turn {turn_idx}: Committed changes to container state.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        logger.error(traceback.format_exc())
    finally:
        # 4. Calculate and Save Chain Metrics
        logger.info(
            f"--- Chain {chain['chain_id']} Finished. Calculating metrics... ---"
        )

        metrics = compute_chain_metrics(chain["task_instances"])

        logger.info(f"Chain Metrics: {json.dumps(metrics, indent=2)}")

        # Save the metrics to a file
        metrics_file = chain_log_dir / "chain_metrics.json"
        metrics_file.write_text(json.dumps(metrics, indent=4))

        # Also save the detailed evaluation results for each turn
        turn_results_file = chain_log_dir / "turn_results.json"
        turn_results = []
        for idx, turn in enumerate(chain["task_instances"]):
            turn_results.append(
                {
                    "turn_index": idx,
                    "instance_id": turn["instance_id"],
                    "evaluation_result": turn.get("evaluation_result", {}),
                }
            )
        turn_results_file.write_text(json.dumps(turn_results, indent=4))

        # clean up the container
        if container:
            logger.info(f"Cleaning up container {container.id}...")
            cleanup_container(client, container, logger)
        close_logger(logger)

        return metrics


def main(
    chains_path: str,
    predictions_path: str,
    run_id: str,
    max_workers: int,
    timeout: int = 1800,
):
    """
    Main function to orchestrate the multi-turn evaluation.

    Args:
        chains_path: Path to chains JSONL file
        predictions_path: Path to predictions file or "gold" for gold patches
        run_id: Unique identifier for this run
        max_workers: Maximum number of parallel workers for building images
        timeout: Timeout for running tests per turn (default 1800 seconds)
    """
    # 1. Load the chains
    chains = load_chains_from_file(chains_path)
    print(f"Found {len(chains)} chains to evaluate.")

    # 2. Load predictions
    predictions = get_chain_predictions(chains, predictions_path)

    # Validate that we have predictions for all chains
    missing_predictions = []
    for chain in chains:
        chain_id = chain["chain_id"]
        if chain_id not in predictions:
            missing_predictions.append(chain_id)

    if missing_predictions:
        print(f"Warning: Missing predictions for {len(missing_predictions)} chains:")
        for chain_id in missing_predictions[:5]:
            print(f"  - {chain_id}")
        if len(missing_predictions) > 5:
            print(f"  ... and {len(missing_predictions) - 5} more")

    # Filter to chains with predictions
    chains_to_evaluate = [c for c in chains if c["chain_id"] in predictions]
    print(f"Will evaluate {len(chains_to_evaluate)} chains with predictions.")

    # 3. Setup Docker client
    client = docker.from_env()

    # 4. Create a log directory for this run
    model_name = "gold" if predictions_path == "gold" else "model"
    if predictions_path != "gold" and chains_to_evaluate:
        first_chain_id = chains_to_evaluate[0]["chain_id"]
        if first_chain_id in predictions and predictions[first_chain_id]:
            model_name = predictions[first_chain_id][0].get(KEY_MODEL, "unknown")

    run_log_dir = MULTI_TURN_LOG_DIR / run_id / model_name.replace("/", "__")
    run_log_dir.mkdir(parents=True, exist_ok=True)
    print(f"Logs will be saved in: {run_log_dir.resolve()}")

    # 5. PRE-BUILD ALL NECESSARY ENVIRONMENT IMAGES
    print("Checking for and building necessary environment images...")
    env_specs = [
        make_test_spec(chain["task_instances"][0]) for chain in chains_to_evaluate
    ]
    successful_builds, failed_builds = build_env_images(
        client=client, dataset=env_specs, force_rebuild=False, max_workers=max_workers
    )
    if failed_builds:
        raise Exception(f"Failed to build {len(failed_builds)} environment images.")

    print(
        f"{len(successful_builds)} environment images are ready. Starting chain evaluation..."
    )

    # 6. Loop through each chain and run it
    all_metrics = []
    for chain in tqdm(chains_to_evaluate, desc="Evaluating chains"):
        try:
            chain_id = chain["chain_id"]
            chain_preds = predictions.get(chain_id, [])
            metrics = run_chain_in_container(
                chain, chain_preds, client, run_id, timeout
            )
            if metrics:
                metrics["chain_id"] = chain_id
                all_metrics.append(metrics)
        except Exception as e:
            print(
                f"ERROR: An unexpected error occurred while running chain {chain['chain_id']}: {e}"
            )
            traceback.print_exc()

    # 7. Save summary metrics
    summary_file = MULTI_TURN_LOG_DIR / run_id / "summary_metrics.json"
    summary = {
        "run_id": run_id,
        "predictions_path": predictions_path,
        "total_chains": len(chains_to_evaluate),
        "chain_metrics": all_metrics,
    }

    # Compute aggregate statistics
    if all_metrics:
        total_turns = sum(m.get("total_turns", 0) for m in all_metrics)
        total_resolved = sum(m.get("resolved_turns", 0) for m in all_metrics)
        full_chain_successes = sum(
            1 for m in all_metrics if m.get("full_chain_success", False)
        )

        summary["aggregate"] = {
            "total_turns": total_turns,
            "total_resolved": total_resolved,
            "overall_success_rate": round(total_resolved / total_turns, 4)
            if total_turns > 0
            else 0,
            "full_chain_success_count": full_chain_successes,
            "full_chain_success_rate": round(full_chain_successes / len(all_metrics), 4)
            if all_metrics
            else 0,
        }

    summary_file.write_text(json.dumps(summary, indent=4))
    print(f"\nSummary metrics saved to: {summary_file}")

    if all_metrics:
        print("\n=== Evaluation Summary ===")
        print(f"Chains evaluated: {len(all_metrics)}")
        print(f"Total turns: {summary.get('aggregate', {}).get('total_turns', 0)}")
        print(
            f"Resolved turns: {summary.get('aggregate', {}).get('total_resolved', 0)}"
        )
        print(
            f"Overall success rate: {summary.get('aggregate', {}).get('overall_success_rate', 0):.2%}"
        )
        print(
            f"Full chain successes: {summary.get('aggregate', {}).get('full_chain_success_count', 0)}"
        )


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Run multi-turn evaluation for SWE-bench chains."
    )
    parser.add_argument(
        "--chains_path",
        type=str,
        required=True,
        help="Path to the .jsonl file containing the multi-turn chains.",
    )
    parser.add_argument(
        "-p",
        "--predictions_path",
        type=str,
        required=True,
        help="Path to predictions file (.json or .jsonl), or 'gold' to use gold patches from chains.",
    )
    parser.add_argument(
        "-id",
        "--run_id",
        type=str,
        required=True,
        help="A unique ID for this evaluation run.",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=4,
        help="Maximum number of parallel workers for building images.",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=1800,
        help="Timeout (in seconds) for running tests for each turn.",
    )
    args = parser.parse_args()
    main(**vars(args))
