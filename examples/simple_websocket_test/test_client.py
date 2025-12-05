#!/usr/bin/env python3
"""Simple test client for WebSocket artifact streaming."""

import base64
import json
import os
from pathlib import Path

import boto3
import httpx
from websockets.sync.client import connect


def main():
    base_url = "http://localhost:8032"

    # Get S3 config from environment
    s3_endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    s3_bucket = os.getenv("S3_BUCKET", "stateful-microservice")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    s3_use_ssl = os.getenv("S3_USE_SSL", "false").lower() == "true"

    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        use_ssl=s3_use_ssl,
    )

    # Ensure bucket exists
    try:
        s3_client.head_bucket(Bucket=s3_bucket)
    except:
        s3_client.create_bucket(Bucket=s3_bucket)
        print(f"Created bucket: {s3_bucket}")

    # Upload dummy file to S3
    dummy_content = b"test websockets"
    s3_key = "datasets/test_websocket_dummy.txt"
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=dummy_content)
    print(f"Uploaded dummy file to s3://{s3_bucket}/{s3_key}")

    # Check health
    response = httpx.get(f"{base_url}/health")
    health = response.json()
    print(f"Service: {health['status']}")

    # Submit a job with the dummy file
    s3_uri = f"s3://{s3_bucket}/{s3_key}"
    response = httpx.post(
        f"{base_url}/jobs",
        json={"manifest_inline": {"files": [s3_uri]}}
    )
    job_id = response.json()["job_id"]
    print(f"Job ID: {job_id}")

    # Connect to WebSocket and receive artifact
    output_dir = Path("./test_output")
    output_dir.mkdir(exist_ok=True)

    ws_url = f"ws://localhost:8032/jobs/{job_id}/progress"

    with connect(ws_url) as ws:
        while True:
            msg = ws.recv()
            message = json.loads(msg)

            msg_type = message.get("type", "progress")
            data = message.get("data", message)

            if msg_type == "progress":
                status = data.get("status")
                progress_info = data.get("progress", {})

                # Show detailed progress information
                if progress_info:
                    stage = progress_info.get("stage", "")
                    percent = progress_info.get("percent")
                    message = progress_info.get("message", "")

                    if percent is not None:
                        print(f"Progress: {stage} - {percent:.1f}%")
                    elif message:
                        print(f"Progress: {stage} - {message}")
                    elif stage:
                        print(f"Progress: {stage}")

                print(f"Status: {status}")

                if status == "completed":
                    result = data.get("result")
                    print(f"Result: {result}")
                    break
                elif status == "failed":
                    print(f"Error: {data.get('error')}")
                    break

            elif msg_type == "artifact":
                # Decode and save artifact
                file_data = base64.b64decode(data["data"])
                name = data["name"]

                output_path = output_dir / name
                with open(output_path, 'wb') as f:
                    f.write(file_data)

                print(f"Received: {name} ({len(file_data)} bytes)")
                print(f"Saved to: {output_path}")

if __name__ == "__main__":
    main()
