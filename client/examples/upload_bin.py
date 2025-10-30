"""Example: Upload local bin files and process them."""

from pathlib import Path
from ifcb_client import IFCBClient

# Create client (debug=True prints detailed upload progress)
client = IFCBClient("http://localhost:8000", debug=True)

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
else:
    print(f"✗ Processing failed: {result.error}")

client.close()
