"""Async usage example for IFCB client."""

import asyncio
from ifcb_client import AsyncIFCBClient, Manifest


async def main():
    """Process multiple jobs concurrently."""

    # Create async client
    async with AsyncIFCBClient("http://localhost:8001") as client:

        # Check health
        health = await client.health()
        print(f"Service: {health.status} (v{health.version})")

        # Submit multiple jobs concurrently
        file_sets = [
            [
                "s3://ifcb-features/data/D20230101T120000_IFCB123.adc",
                "s3://ifcb-features/data/D20230101T120000_IFCB123.roi",
                "s3://ifcb-features/data/D20230101T120000_IFCB123.hdr",
            ],
            [
                "s3://ifcb-features/data/D20230101T130000_IFCB123.adc",
                "s3://ifcb-features/data/D20230101T130000_IFCB123.roi",
                "s3://ifcb-features/data/D20230101T130000_IFCB123.hdr",
            ],
            [
                "s3://ifcb-features/data/D20230101T140000_IFCB123.adc",
                "s3://ifcb-features/data/D20230101T140000_IFCB123.roi",
                "s3://ifcb-features/data/D20230101T140000_IFCB123.hdr",
            ],
        ]

        tasks = []
        for files in file_sets:
            manifest = Manifest(files=files)
            task = client.submit_job(manifest_inline=manifest)
            tasks.append(task)

        # Submit all jobs concurrently
        jobs = await asyncio.gather(*tasks)

        print(f"Submitted {len(jobs)} jobs")
        for job in jobs:
            print(f"  - {job.job_id}")

        # Wait for all jobs to complete
        print("\nWaiting for jobs to complete...")
        wait_tasks = [client.wait_for_job(job.job_id) for job in jobs]
        results = await asyncio.gather(*wait_tasks)

        # Print results
        print("\nResults:")
        for result in results:
            if result.status == "completed":
                print(f"✓ {result.job_id}: completed")
            else:
                print(f"✗ {result.job_id}: {result.error}")


if __name__ == "__main__":
    asyncio.run(main())
