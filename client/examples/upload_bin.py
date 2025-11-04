"""Example: Upload IFCB bins from a directory and track processing progress."""

import argparse
import json
import time
from pathlib import Path

from ifcb_client import IFCBClient, discover_bins


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload IFCB bins from a directory.")
    parser.add_argument(
        "directory",
        type=Path,
        help="Directory containing IFCB bin files (.adc/.roi/.hdr).",
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8001",
        help="Base URL of the IFCB service (default: %(default)s).",
    )
    parser.add_argument(
        "--s3-endpoint-url",
        default=None,
        help="Optional S3 endpoint override used by the service.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively search for bins under the directory.",
    )
    parser.add_argument(
        "--skip-incomplete",
        action="store_true",
        help="Skip bins that are missing required files instead of failing.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./downloads"),
        help="Directory where job outputs should be downloaded (default: %(default)s).",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Do not download results after the job completes.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose client logging.",
    )
    return parser.parse_args()


def print_progress(progress: dict) -> None:
    snapshot = json.dumps(progress, sort_keys=True)
    stage = progress.get("stage", "processing")
    percent = progress.get("percent")
    detail = progress.get("detail") or {}

    bin_id = detail.get("bin_id")
    bin_percent = detail.get("bin_percent")
    message = detail.get("message") or progress.get("message")
    processed_rois = detail.get("processed_rois")
    total_rois = detail.get("total_rois")

    line = f"  [{stage}]"
    if percent is not None:
        line += f" {percent:.1f}%"

    if bin_id:
        line += f" (bin {bin_id}"
        if bin_percent is not None:
            line += f" {bin_percent:.1f}%"
        if detail.get("bin_index") and detail.get("bin_total"):
            line += f" {detail['bin_index']}/{detail['bin_total']}"
        line += ")"

    if processed_rois and total_rois:
        roi_percent = (processed_rois / total_rois) * 100.0
        line += f" {processed_rois}/{total_rois} ROIs ({roi_percent:.1f}%)"

    if message:
        line += f" - {message}"

    print(line)
    return snapshot


def main() -> None:
    args = parse_args()

    directory = args.directory.expanduser().resolve()
    if not directory.exists() or not directory.is_dir():
        raise SystemExit(f"Directory not found: {directory}")

    print(f"Scanning directory: {directory}")
    bins = discover_bins(
        directory,
        recursive=args.recursive,
        skip_incomplete=args.skip_incomplete,
    )

    if not bins:
        raise SystemExit("No complete bins found.")

    print(f"Found {len(bins)} bin(s). Starting upload...")

    sample_bins = list(bins.items())
    preview_count = min(len(sample_bins), 10)
    for bin_id, files in sample_bins[:preview_count]:
        file_list = ", ".join(sorted(path.name for path in files.values()))
        print(f"  - {bin_id}: {file_list}")
    if len(sample_bins) > preview_count:
        print(f"  ... and {len(sample_bins) - preview_count} more bin(s)")

    client_kwargs = {}
    if args.s3_endpoint_url:
        client_kwargs["s3_endpoint_url"] = args.s3_endpoint_url

    client = IFCBClient(args.base_url, debug=args.debug, **client_kwargs)

    try:
        job_id = client.upload_bins(bins)
        print(f"✓ Upload initiated. Job ID: {job_id}")

        print("Processing...")
        last_progress_snapshot = None

        while True:
            status = client.get_job(job_id)
            if status.progress:
                snapshot = json.dumps(status.progress, sort_keys=True)
                if snapshot != last_progress_snapshot:
                    last_progress_snapshot = print_progress(status.progress)

            if status.status in {"completed", "failed"}:
                result = status
                break

            time.sleep(1)

        if result.status == "completed":
            print("✓ Processing complete!")
            print(f"  Bins: {result.result.counts.bins}")
            print(f"  ROIs: {result.result.counts.rois}")
            print(f"  Features sample: {result.result.features.uris[0]}")

            if not args.no_download:
                output_dir = (args.output_dir / job_id).resolve()
                downloads = client.download_results(job_id, output_dir, overwrite=True)

                print(f"Downloaded artifacts to {output_dir}:")
                for category, paths in downloads.items():
                    for path in paths:
                        print(f"  [{category}] {path}")
        else:
            print(f"✗ Processing failed: {result.error}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
