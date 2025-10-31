"""Example: Upload local bin files and process them."""

from pathlib import Path
from ifcb_client import IFCBClient

# Create client (debug=True prints detailed upload progress)
client = IFCBClient("http://localhost:8000", s3_endpoint_url="", debug=True)

# Upload bin files from local disk
bin_id = ""
file_paths = {
    '.adc': Path(f""),
    '.hdr': Path(f""),
    '.roi': Path(f"")
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
result = client.wait_for_job(job_id, poll_interval=5)

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
