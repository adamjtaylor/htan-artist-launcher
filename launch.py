#!/usr/bin/env python3
"""
HTAN Artist Pipeline Launcher

This script uploads a samplesheet to Seqera Platform and launches the nf-artist pipeline.
"""

import argparse
import logging
import re
import string
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import shortuuid
from dotenv import load_dotenv
from seqerakit import seqeraplatform

# Load environment variables
load_dotenv()

# Constants
DEFAULT_WORKSPACE = "253119656982040"  # htan-project
DEFAULT_COMPUTE_ENV = "3k6bQPIqpII2nGlDMbc9Mv"  # htan-project-spot-v12
DEFAULT_API_URL = "https://tower.sagebionetworks.org/api"
DEFAULT_PIPELINE_URL = "https://github.com/Sage-Bionetworks-Workflows/nf-artist"
DEFAULT_SAMPLESHEET = "samplesheet/artist-samplesheet-sample10.csv"
DEFAULT_OUTPUT_BUCKET = "s3://htan-project-tower-bucket/outputs"

# Initialize short UUID generator
alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
shortuuid_generator = shortuuid.ShortUUID(alphabet=alphabet)


def setup_logging(debug: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def generate_run_id() -> str:
    """Generate a unique 4-character identifier for the run."""
    return shortuuid_generator.random(length=4)


def validate_samplesheet(samplesheet_path: Path) -> None:
    """Validate that the samplesheet exists and has required columns."""
    if not samplesheet_path.exists():
        raise FileNotFoundError(f"Samplesheet not found: {samplesheet_path}")

    try:
        df = pd.read_csv(samplesheet_path)
        required_columns = ["id", "image"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in samplesheet: {missing_columns}"
            )

        if df.empty:
            raise ValueError("Samplesheet is empty")

        logging.info(f"Validated samplesheet with {len(df)} rows")
    except Exception as e:
        raise ValueError(f"Invalid samplesheet format: {e}")


def extract_dataset_id(response: str) -> str:
    """Extract dataset ID from Seqera Platform response."""
    pattern = r"with id '(.+?)'"
    match = re.search(pattern, response)
    if not match:
        raise ValueError(f"Dataset ID not found in response: {response}")
    return match.group(1)


def extract_url(text: str) -> str:
    """Extract the first URL from a given string."""
    url_pattern = r"(https?://[^\s]+)"
    match = re.search(url_pattern, text)
    if not match:
        raise ValueError(f"No URL found in text: {text}")
    return match.group(0)


def upload_dataset(
    platform: seqeraplatform.SeqeraPlatform,
    samplesheet_path: Path,
    workspace: str,
    dataset_name: str,
) -> str:
    """Upload samplesheet as a dataset to Seqera Platform."""
    logging.info(f"Uploading dataset '{dataset_name}' from {samplesheet_path}")

    response = platform.datasets(
        "add",
        str(samplesheet_path),
        "--workspace",
        workspace,
        "--name",
        dataset_name,
        "--description",
        "Samplesheet for artist pipeline",
        "--overwrite",
        "--header",
        to_json=True,
    )

    dataset_id = extract_dataset_id(response)
    logging.info(f"Dataset uploaded with ID: {dataset_id}")
    return dataset_id


def get_dataset_url(
    platform: seqeraplatform.SeqeraPlatform, dataset_id: str, workspace: str
) -> str:
    """Get the URL for an uploaded dataset."""
    logging.info(f"Getting URL for dataset {dataset_id}")

    response = platform.datasets(
        "url",
        "--id",
        dataset_id,
        "--workspace",
        workspace,
        to_json=True,
    )

    url = extract_url(response)
    logging.info(f"Dataset URL: {url}")
    return url


def create_params_file(
    dataset_url: str, run_name: str, output_bucket: str, params_dir: Path
) -> Path:
    """Create a parameters file for the pipeline."""
    params_dir.mkdir(exist_ok=True)

    params_content = f"""input: {dataset_url}
outdir: "{output_bucket}/{run_name}"
"""

    params_file = params_dir / f"params_{run_name.split('_')[1]}.yaml"
    params_file.write_text(params_content)

    logging.info(f"Created parameters file: {params_file}")
    return params_file


def launch_pipeline(
    platform: seqeraplatform.SeqeraPlatform,
    workspace: str,
    compute_env: str,
    run_name: str,
    params_file: Path,
    pipeline_url: str,
    revision: str = "main",
) -> str:
    """Launch the nf-artist pipeline."""
    logging.info(f"Launching pipeline run '{run_name}'")

    response = platform.launch(
        "--workspace",
        workspace,
        "--compute-env",
        compute_env,
        "--name",
        run_name,
        "--revision",
        revision,
        "--wait",
        "SUBMITTED",
        pipeline_url,
        "--profile",
        "tower",
        "--params-file",
        str(params_file),
        to_json=True,
    )

    logging.info("Pipeline launched successfully")
    return response


def cleanup_params_file(params_file: Path) -> None:
    """Remove the parameters file if it exists."""
    try:
        if params_file.exists():
            params_file.unlink()
            logging.info(f"Removed parameters file: {params_file}")
        else:
            logging.warning(f"Parameters file not found for cleanup: {params_file}")
    except Exception as e:
        logging.warning(f"Failed to remove parameters file {params_file}: {e}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Launch HTAN Artist Pipeline on Seqera Platform",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--samplesheet",
        type=Path,
        default=DEFAULT_SAMPLESHEET,
        help="Path to the samplesheet CSV file",
    )

    parser.add_argument(
        "--workspace", default=DEFAULT_WORKSPACE, help="Seqera Platform workspace ID"
    )

    parser.add_argument(
        "--compute-env", default=DEFAULT_COMPUTE_ENV, help="Compute environment ID"
    )

    parser.add_argument(
        "--output-bucket",
        default=DEFAULT_OUTPUT_BUCKET,
        help="S3 bucket for output files",
    )

    parser.add_argument(
        "--pipeline-url",
        default=DEFAULT_PIPELINE_URL,
        help="GitHub URL of the nf-artist pipeline",
    )

    parser.add_argument(
        "--revision", default="main", help="Pipeline revision/branch to use"
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    parser.add_argument(
        "--keep-params",
        action="store_true",
        help="Keep parameter file after pipeline launch (default: remove it)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.debug)

    try:
        # Validate inputs
        validate_samplesheet(args.samplesheet)

        # Generate unique identifiers
        run_id = generate_run_id()
        dataset_name = f"artist_samplesheet_{run_id}"
        run_name = f"artist_{run_id}"

        logging.info(f"Starting pipeline launch with run ID: {run_id}")

        # Initialize Seqera Platform
        platform = seqeraplatform.SeqeraPlatform()

        # Upload dataset
        dataset_id = upload_dataset(
            platform, args.samplesheet, args.workspace, dataset_name
        )

        # Get dataset URL
        dataset_url = get_dataset_url(platform, dataset_id, args.workspace)

        # Create parameters file
        params_dir = Path("params")
        params_file = create_params_file(
            dataset_url, run_name, args.output_bucket, params_dir
        )

        # Launch pipeline
        result = launch_pipeline(
            platform,
            args.workspace,
            args.compute_env,
            run_name,
            params_file,
            args.pipeline_url,
            args.revision,
        )

        print(f"✓ Pipeline launched successfully!")
        print(f"  Run name: {run_name}")
        print(f"  Dataset ID: {dataset_id}")
        print(f"  Parameters file: {params_file}")
        print(f"  Result: {result}")

        # Clean up parameters file unless user wants to keep it
        if args.keep_params:
            logging.info(f"Keeping parameters file: {params_file}")
        else:
            cleanup_params_file(params_file)

        return 0

    except Exception as e:
        logging.error(f"Pipeline launch failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
