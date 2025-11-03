"""S3 storage client for handling multipart uploads and object management."""

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime, timedelta

from .config import settings

logger = logging.getLogger(__name__)


class S3Client:
    """S3 client wrapper for local S3-compatible storage (MinIO, etc.)."""

    def __init__(self):
        """Initialize S3 client with local endpoint configuration."""
        self.client = boto3.client(
            's3',
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(signature_version='s3v4'),
            use_ssl=settings.s3_use_ssl,
        )
        self.bucket = settings.s3_bucket
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket '{self.bucket}' exists")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Creating bucket '{self.bucket}'")
                self.client.create_bucket(Bucket=self.bucket)
            else:
                raise

    def create_multipart_upload(self, key: str) -> str:
        """
        Create a multipart upload and return the upload ID.

        Args:
            key: S3 object key

        Returns:
            Upload ID string
        """
        try:
            response = self.client.create_multipart_upload(
                Bucket=self.bucket,
                Key=key,
            )
            upload_id = response['UploadId']
            logger.info(f"Created multipart upload for {key}: {upload_id}")
            return upload_id
        except ClientError as e:
            logger.error(f"Failed to create multipart upload for {key}: {e}")
            raise

    def generate_presigned_part_urls(
        self,
        key: str,
        upload_id: str,
        num_parts: int,
        ttl_seconds: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate pre-signed URLs for all parts of a multipart upload.

        Args:
            key: S3 object key
            upload_id: Multipart upload ID
            num_parts: Number of parts to generate URLs for
            ttl_seconds: URL expiration time (defaults to config setting)

        Returns:
            List of dicts with part_number and url
        """
        if ttl_seconds is None:
            ttl_seconds = settings.multipart_url_ttl_seconds

        urls = []
        for part_number in range(1, num_parts + 1):
            url = self.client.generate_presigned_url(
                'upload_part',
                Params={
                    'Bucket': self.bucket,
                    'Key': key,
                    'UploadId': upload_id,
                    'PartNumber': part_number,
                },
                ExpiresIn=ttl_seconds,
            )
            urls.append({
                'part_number': part_number,
                'url': url,
            })

        logger.info(f"Generated {num_parts} pre-signed URLs for {key}")
        return urls

    def complete_multipart_upload(
        self,
        key: str,
        upload_id: str,
        parts: List[Dict[str, Any]],
    ) -> str:
        """
        Complete a multipart upload.

        Args:
            key: S3 object key
            upload_id: Multipart upload ID
            parts: List of dicts with PartNumber and ETag

        Returns:
            Object ETag
        """
        try:
            response = self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
            )
            logger.info(f"Completed multipart upload for {key}")
            return response['ETag']
        except ClientError as e:
            logger.error(f"Failed to complete multipart upload for {key}: {e}")
            raise

    def abort_multipart_upload(self, key: str, upload_id: str):
        """
        Abort a multipart upload (cleanup).

        Args:
            key: S3 object key
            upload_id: Multipart upload ID
        """
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
            )
            logger.info(f"Aborted multipart upload for {key}")
        except ClientError as e:
            logger.warning(f"Failed to abort multipart upload for {key}: {e}")

    def object_exists(self, key: str) -> bool:
        """
        Check if an object exists in S3.

        Args:
            key: S3 object key

        Returns:
            True if object exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def get_object_url(self, key: str) -> str:
        """
        Get the S3 URI for an object.

        Args:
            key: S3 object key

        Returns:
            S3 URI (s3://bucket/key)
        """
        return f"s3://{self.bucket}/{key}"

    def upload_fileobj(self, fileobj, key: str):
        """
        Upload a file object directly (for small files).

        Args:
            fileobj: File-like object
            key: S3 object key
        """
        try:
            self.client.upload_fileobj(fileobj, self.bucket, key)
            logger.info(f"Uploaded object to {key}")
        except ClientError as e:
            logger.error(f"Failed to upload object to {key}: {e}")
            raise

    def download_fileobj(self, key: str, fileobj):
        """
        Download an object to a file object.

        Args:
            key: S3 object key
            fileobj: File-like object to write to
        """
        try:
            self.client.download_fileobj(self.bucket, key, fileobj)
            logger.info(f"Downloaded object from {key}")
        except ClientError as e:
            logger.error(f"Failed to download object from {key}: {e}")
            raise

    def list_objects(self, prefix: str, max_keys: int = 1000) -> List[str]:
        """
        List objects with a given prefix.

        Args:
            prefix: S3 key prefix
            max_keys: Maximum number of keys to return

        Returns:
            List of object keys
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=max_keys,
            )
            if 'Contents' not in response:
                return []
            return [obj['Key'] for obj in response['Contents']]
        except ClientError as e:
            logger.error(f"Failed to list objects with prefix {prefix}: {e}")
            raise


# Global S3 client instance
s3_client = S3Client()
