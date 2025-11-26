import json
import traceback
from argparse import ArgumentParser
from pathlib import Path

import docker
from tqdm import tqdm

from swebench.harness.constants import LOG_TEST_OUTPUT
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


def run_chain_in_container(chain: dict, client: docker.DockerClient, run_id: str):
    """
    Runs a single multi-turn chain within a dedicated Docker container.
    """
    chain_id = chain["chain_id"]
    print(f"\n--- Running Chain: {chain_id} ---")

    # Setup logging for this specific chain
    chain_log_dir = MULTI_TURN_LOG_DIR / run_id / chain_id
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

        # 3. Loop through each turn
        for turn_idx, turn in enumerate(chain["task_instances"]):
            instance_id = turn["instance_id"]
            logger.info(f"--- Starting Turn {turn_idx} (Instance: {instance_id}) ---")

            # a. Apply the patch for the current turn
            patch_content = turn["patch"]

            if not patch_content.endswith("\n"):
                patch_content += "\n"

            patch_path = Path(chain_log_dir / f"turn_{turn_idx}_patch.diff")
            patch_path.write_text(patch_content)

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
                    f"Turn {turn_idx}: All patch application methods FAILED. Aborting chain."
                )
                break  # Stop processing this chain

            # b. Prepare and Run the Test Script
            logger.info(f"Turn {turn_idx}: Generating evaluation script...")

            # Create a TestSpec for this specific turn to generate the correct eval script
            # This uses the instance metadata to build the exact bash commands needed (reset tests, apply test patch, run pytest)
            current_turn_spec = make_test_spec(turn)

            # Write the script to a local file first
            eval_script_content = current_turn_spec.eval_script
            eval_script_path = Path(chain_log_dir / f"turn_{turn_idx}_eval.sh")
            eval_script_path.write_text(eval_script_content)

            # Copy it into the container
            container_eval_path = Path("/eval.sh")
            copy_to_container(container, eval_script_path, container_eval_path)

            # Execute the script inside the container
            logger.info(f"Turn {turn_idx}: Running tests (timeout=1800s)...")
            try:
                # Executing /bin/bash /eval.sh inside the persistent container
                test_output, timed_out, total_runtime = exec_run_with_timeout(
                    container, "/bin/bash /eval.sh", timeout=1800
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

            # Construct a dummy 'prediction' object because get_eval_report expects one
            # to verify the structure, even though we are running gold patches here.
            prediction_wrapper = {
                "instance_id": turn["instance_id"],
                "model_patch": patch_content,
                "model_name_or_path": "multi_turn_gold",
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

        # clean up the container
        if container:
            logger.info(f"Cleaning up container {container.id}...")
            cleanup_container(client, container, logger)
        close_logger(logger)


def main(chains_path: str, run_id: str, max_workers: int):
    """
    Main function to orchestrate the multi-turn evaluation.
    """
    # 1. Load the chains
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
            print(f"Finished parsing {len(chains)} JSON objects.")
            break

    print(f"Found {len(chains)} chains to evaluate.")

    # 2. Setup Docker client
    client = docker.from_env()

    # 3. Create a log directory for this run
    run_log_dir = MULTI_TURN_LOG_DIR / run_id
    run_log_dir.mkdir(parents=True, exist_ok=True)
    print(f"Logs will be saved in: {run_log_dir.resolve()}")

    # 4. PRE-BUILD ALL NECESSARY ENVIRONMENT IMAGES
    print("Checking for and building necessary environment images...")
    # Create a spec for the first turn of each chain to represent the environment needed
    env_specs = [make_test_spec(chain["task_instances"][0]) for chain in chains]
    successful_builds, failed_builds = build_env_images(
        client=client, dataset=env_specs, force_rebuild=False, max_workers=max_workers
    )
    if failed_builds:
        raise Exception(f"Failed to build {len(failed_builds)} environment images.")

    print(
        f"{len(successful_builds)} environment images are ready. Starting chain evaluation..."
    )

    # 5. Loop through each chain and run it
    for chain in tqdm(chains, desc="Evaluating chains"):
        try:
            run_chain_in_container(chain, client, run_id)
        except Exception as e:
            print(
                f"ERROR: An unexpected error occurred while running chain {chain['chain_id']}: {e}"
            )
            # Continue to the next chain


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
    args = parser.parse_args()
    main(**vars(args))
