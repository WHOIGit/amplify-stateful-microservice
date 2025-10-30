"""Basic usage examples for IFCB client."""

from ifcb_client import IFCBClient, Manifest, BinManifestEntry

# Create a client
client = IFCBClient("http://localhost:8000")

# Check health
health = client.health()
print(f"Service: {health.status} (v{health.version})")

# Submit a job with inline manifest
manifest = Manifest(bins=[
    BinManifestEntry(
        bin_id="D20230101T120000_IFCB123",
        files=[
            "s3://ifcb-features/data/D20230101T120000_IFCB123.adc",
            "s3://ifcb-features/data/D20230101T120000_IFCB123.roi",
            "s3://ifcb-features/data/D20230101T120000_IFCB123.hdr",
        ],
        bytes=5000000,
    )
])

job = client.submit_job(manifest_inline=manifest)
print(f"Submitted job: {job.job_id}")

# Wait for completion
print("Waiting for job to complete...")
result = client.wait_for_job(job.job_id, poll_interval=5)

if result.status == "completed":
    print(f"✓ Job completed!")
    print(f"  Bins processed: {result.result.counts.bins}")
    print(f"  ROIs extracted: {result.result.counts.rois}")
    print(f"  Masks generated: {result.result.counts.masks}")
    print(f"  Features: {result.result.features.uris}")
    print(f"  Masks: {len(result.result.masks.shards)} shards")
else:
    print(f"✗ Job failed: {result.error}")

# List recent jobs
print("\nRecent jobs:")
for job in client.list_jobs(limit=5):
    print(f"  {job.job_id}: {job.status}")

client.close()
