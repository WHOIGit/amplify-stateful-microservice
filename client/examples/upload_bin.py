"""Example: Upload local bin files, track progress, and download results."""

import json
import time
from pathlib import Path

from ifcb_client import IFCBClient


# Create client (debug=True prints detailed upload progress)
client = IFCBClient("http://localhost:8000", s3_endpoint_url="", debug=True)

# Upload bin files from local disk
bin_id = ""
file_paths = {
    '.adc': Path(""),
    '.hdr': Path(""),
    '.roi': Path(""),
}

print(f"Uploading bin: {bin_id}")
print(f"  Files: {list(f.name for f in file_paths.values())}")

# This handles:
# 1. Initiating multipart uploads
# 2. Uploading all parts
# 3. Completing uploads
# 4. Creating the job
job_id = client.upload_bin(bin_id, file_paths)

print(f"✓ Upload complete. Job ID: {job_id}")

# Wait for processing
print("Processing...")
last_progress_snapshot = None

while True:
    status = client.get_job(job_id)
    if status.progress:
        progress = status.progress
        snapshot = json.dumps(progress, sort_keys=True)

        if snapshot != last_progress_snapshot:
            last_progress_snapshot = snapshot

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

    if status.status in {"completed", "failed"}:
        result = status
        break

    time.sleep(1)

if result.status == "completed":
    print(f"✓ Processing complete!")
    print(f"  ROIs: {result.result.counts.rois}")
    print(f"  Features: {result.result.features.uris[0]}")

    # Download results from S3 to local disk
    output_dir = Path("./downloads")
    downloads = client.download_results(job_id, output_dir, overwrite=True)

    print(f"Downloaded artifacts to {output_dir}:")
    for category, paths in downloads.items():
        for path in paths:
            print(f"  [{category}] {path}")
else:
    print(f"✗ Processing failed: {result.error}")

client.close()
