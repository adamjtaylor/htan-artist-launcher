#!/usr/bin/env python3
"""
HTAN Imaging Assets Shortlist Generator

This script fetches imaging assets data from S3, queries BigQuery for released files,
and generates samplesheets for files that need thumbnail or Minerva processing.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd
from google.cloud import bigquery

# Constants
DEFAULT_ASSETS_URI = "s3://htan-assets/final-output/htan-imaging-assets-latest.json"
DEFAULT_AWS_PROFILE = "htan-dev"
DEFAULT_BQ_PROJECT = "htan-dcc"
DEFAULT_OUTPUT_DIR = Path("samplesheet")
DEFAULT_SAMPLE_SIZE = 10

#
# Query retrieves released ImagingLevel2 entities from BigQuery.
# Joins the entities table with ImagingLevel2 assay metadata to pull:
#   • entityId (id)
#   • cloud storage path of the image
#   • a boolean flag for H&E assay type
#   • a convert flag (true if not an OME-TIFF file)
#   • imaging assay type
#   • originating HTAN center
# Filters out MERFISH assay types and data from the HTAPP center.
BIGQUERY_QUERY = """
SELECT 
    e.entityId as id, 
    i2.Cloud_Storage_Path AS image,
    i2.Imaging_Assay_Type = 'H&E' AS he, 
    CASE
        WHEN REGEXP_CONTAINS(i2.Filename, r'\\.ome\\.tif{1,2}$') THEN FALSE
        ELSE TRUE
    END AS convert,
    i2.Imaging_Assay_Type AS type,
    i2.HTAN_Center AS center
FROM `htan-dcc.released.entities` e
LEFT JOIN `htan-dcc.combined_assays.ImagingLevel2` i2
ON e.entityId = i2.entityId
WHERE e.Component = 'ImagingLevel2' 
    AND i2.Imaging_Assay_Type != 'MERFISH'
    AND i2.HTAN_Center != 'HTAN HTAPP'
"""


def setup_logging(debug: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse S3 URI into bucket and key components."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")

    parts = uri.replace("s3://", "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 URI format: {uri}")

    return parts[0], parts[1]


def fetch_assets_data(
    uri: str, aws_profile: str, output_file: Optional[Path] = None
) -> List[Dict]:
    """Fetch imaging assets data from S3."""
    logging.info(f"Fetching assets data from {uri}")

    try:
        session = boto3.Session(profile_name=aws_profile)
        s3 = session.client("s3")

        bucket, key = parse_s3_uri(uri)
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read())

        logging.info(f"Successfully fetched {len(data)} asset records")

        # Save to file if requested
        if output_file:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            logging.info(f"Saved assets data to {output_file}")

        return data

    except Exception as e:
        logging.error(f"Failed to fetch assets data: {e}")
        raise


def extract_existing_assets(data: List[Dict]) -> Tuple[List[str], List[str]]:
    """Extract lists of synids that already have thumbnails and Minerva views."""
    has_thumbnail = []
    has_minerva = []

    for item in data:
        synid = item.get("synid")
        if not synid:
            continue

        if "thumbnail" in item:
            has_thumbnail.append(synid)
        if "minerva" in item:
            has_minerva.append(synid)

    logging.info(f"Found {len(has_thumbnail)} synids with thumbnails")
    logging.info(f"Found {len(has_minerva)} synids with Minerva views")

    return has_thumbnail, has_minerva


def query_bigquery_data(project_id: str) -> pd.DataFrame:
    """Query BigQuery for released imaging files."""
    logging.info(f"Querying BigQuery project: {project_id}")

    try:
        client = bigquery.Client(project=project_id)
        query_job = client.query(BIGQUERY_QUERY)
        results = query_job.result()
        df = results.to_dataframe()

        logging.info(f"Retrieved {len(df)} records from BigQuery")
        return df

    except Exception as e:
        logging.error(f"BigQuery query failed: {e}")
        raise


def process_dataframe(
    df: pd.DataFrame,
    has_thumbnail: List[str],
    has_minerva: List[str],
    assets_data: List[Dict],
) -> pd.DataFrame:
    """Process the dataframe to add missing asset flags and URIs."""
    logging.info("Processing dataframe...")

    # Determine which files need processing
    df["miniature"] = ~df["id"].isin(has_thumbnail)
    df["minerva"] = ~df["id"].isin(has_minerva)

    # Create lookup dictionaries for existing URIs
    minerva_dict = {item["synid"]: item.get("minerva", "") for item in assets_data}
    thumbnail_dict = {item["synid"]: item.get("thumbnail", "") for item in assets_data}

    # df["minerva_uri"] = df["id"].map(minerva_dict).fillna("")
    # df["miniature_uri"] = df["id"].map(thumbnail_dict).fillna("")

    # Extract cloud provider from image path
    df["cloud_provider"] = df["image"].str.extract(r"(s3|gs)://")

    # Filter for files that need at least one type of processing
    df_filtered = df[(df["miniature"]) | (df["minerva"])].copy()

    # Sort by synid (descending)
    df_filtered = df_filtered.sort_values(by="id", ascending=False)

    logging.info(f"Filtered to {len(df_filtered)} files needing processing")
    logging.info(f"Files needing thumbnails: {df_filtered['miniature'].sum()}")
    logging.info(f"Files needing Minerva views: {df_filtered['minerva'].sum()}")

    return df_filtered


def print_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics about the dataset."""
    print(f"\n📊 Dataset Statistics")
    print("=" * 50)
    print(f"Total files needing processing: {len(df)}")
    print(f"Files needing thumbnails: {df['miniature'].sum()}")
    print(f"Files needing Minerva views: {df['minerva'].sum()}")

    print(f"\n🔬 Counts by imaging type:")
    print("-" * 30)
    type_counts = df["type"].value_counts()
    for img_type, count in type_counts.items():
        print(f"  {img_type}: {count}")

    print(f"\n🏥 Counts by center:")
    print("-" * 20)
    center_counts = df["center"].value_counts()
    for center, count in center_counts.items():
        print(f"  {center}: {count}")

    print("\n📋 Sample records:")
    print("-" * 20)
    sample_df = df.sample(min(5, len(df)))
    for _, row in sample_df.iterrows():
        print(f"  {row['id']}: {row['type']} from {row['center']}")


def save_samplesheets(df: pd.DataFrame, output_dir: Path, sample_size: int) -> None:
    """Save full and sample samplesheets."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save full samplesheet
    full_path = output_dir / "artist-samplesheet.csv"
    df.to_csv(full_path, index=False)
    logging.info(f"Saved full samplesheet with {len(df)} records to {full_path}")

    # Save sample samplesheet
    if len(df) > sample_size:
        df_sample = df.sample(sample_size)
        sample_path = output_dir / f"artist-samplesheet-sample{sample_size}.csv"
        df_sample.to_csv(sample_path, index=False)
        logging.info(
            f"Saved sample samplesheet with {sample_size} records to {sample_path}"
        )
    else:
        logging.info(f"Dataset has {len(df)} records, no separate sample needed")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate HTAN imaging samplesheets for missing assets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--assets-uri",
        default=DEFAULT_ASSETS_URI,
        help="S3 URI for the imaging assets JSON file",
    )

    parser.add_argument(
        "--aws-profile",
        default=DEFAULT_AWS_PROFILE,
        help="AWS profile name for S3 access",
    )

    parser.add_argument(
        "--bigquery-project",
        default=DEFAULT_BQ_PROJECT,
        help="Google Cloud BigQuery project ID",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for samplesheets",
    )

    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of records in sample samplesheet",
    )

    parser.add_argument(
        "--cloud-provider",
        choices=["s3", "gs", "all"],
        default="all",
        help="Filter by cloud provider (s3, gs, or all)",
    )

    parser.add_argument(
        "--save-assets",
        action="store_true",
        help="Save the raw assets JSON file locally",
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.debug)

    try:
        # Fetch assets data
        assets_file = (
            Path("htan-imaging-assets-latest.json") if args.save_assets else None
        )
        assets_data = fetch_assets_data(args.assets_uri, args.aws_profile, assets_file)

        # Extract existing assets
        has_thumbnail, has_minerva = extract_existing_assets(assets_data)

        # Query BigQuery
        df = query_bigquery_data(args.bigquery_project)

        # Process dataframe
        df_processed = process_dataframe(df, has_thumbnail, has_minerva, assets_data)

        # Filter by cloud provider if specified
        if args.cloud_provider != "all":
            initial_count = len(df_processed)
            df_processed = df_processed[
                df_processed["cloud_provider"] == args.cloud_provider
            ]
            logging.info(
                f"Filtered by {args.cloud_provider}: {initial_count} → {len(df_processed)} records"
            )

        if df_processed.empty:
            print("✓ No files need processing!")
            return 0

        # Print statistics
        print_statistics(df_processed)

        # Save samplesheets
        save_samplesheets(df_processed, args.output_dir, args.sample_size)

        print(f"\n✓ Samplesheets saved to {args.output_dir}")
        return 0

    except Exception as e:
        logging.error(f"Script failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
