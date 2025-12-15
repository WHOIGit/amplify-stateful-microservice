"""Storage adapter for amplify-storage-utils with S3 multipart support."""

import io
import logging
from typing import List, Dict, Optional, Any

from storage import ObjectStore
from storage.s3 import BucketStore
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class StorageAdapter:
    """
    Adapts amplify-storage-utils ObjectStore to microservice needs.

    Provides a unified interface that supports both generic ObjectStore operations
    and S3-specific features like multipart uploads and presigned URLs.
    """

    def __init__(self, store: ObjectStore, bucket_name: str):
        """
        Initialize storage adapter.

        Args:
            store: ObjectStore instance from amplify-storage-utils
            bucket_name: Bucket/namespace name (used for S3 URIs)
        """
        self.store = store
        self.bucket_name = bucket_name
        self._is_s3 = isinstance(store, BucketStore)
        self._s3_client = store.s3_client if self._is_s3 else None

    def put_bytes(self, key: str, data: bytes) -> None:
        """
        Store bytes at key.

        Args:
            key: Storage key
            data: Bytes to store
        """
        self.store.put(key, data)
        logger.info(f"Stored {len(data)} bytes at {key}")

    def get_bytes(self, key: str) -> bytes:
        """
        Retrieve bytes from key.

        Args:
            key: Storage key

        Returns:
            Stored bytes
        """
        data = self.store.get(key)
        logger.info(f"Retrieved {len(data)} bytes from {key}")
        return data

    def upload_fileobj(self, fileobj, key: str) -> None:
        """
        Upload a file object to storage.

        Args:
            fileobj: File-like object to upload
            key: Storage key
        """
        if self._is_s3:
            # Use boto3 upload_fileobj for S3
            self._s3_client.upload_fileobj(fileobj, self.bucket_name, key)
            logger.info(f"Uploaded file object to {key} (S3)")
        else:
            # Read entire file and use ObjectStore.put
            data = fileobj.read()
            self.store.put(key, data)
            logger.info(f"Uploaded {len(data)} bytes to {key}")

    def download_fileobj(self, key: str, fileobj) -> None:
        """
        Download object to file object.

        Args:
            key: Storage key
            fileobj: File-like object to write to
        """
        if self._is_s3:
            # Use boto3 download_fileobj for S3
            self._s3_client.download_fileobj(self.bucket_name, key, fileobj)
            logger.info(f"Downloaded {key} to file object (S3)")
        else:
            # Get data and write to file object
            data = self.store.get(key)
            fileobj.write(data)
            logger.info(f"Downloaded {len(data)} bytes from {key} to file object")

    def exists(self, key: str) -> bool:
        """
        Check if key exists in storage.

        Args:
            key: Storage key

        Returns:
            True if key exists
        """
        return self.store.exists(key)

    def delete(self, key: str) -> None:
        """
        Delete object at key.

        Args:
            key: Storage key
        """
        self.store.delete(key)
        logger.info(f"Deleted {key}")

    # =========================================================================
    # S3-Specific Multipart Upload Operations
    # =========================================================================

    def create_multipart_upload(self, key: str) -> str:
        """
        Create a multipart upload (S3 only).

        Args:
            key: S3 object key

        Returns:
            Upload ID string

        Raises:
            NotImplementedError: If backend is not S3
        """
        if not self._is_s3:
            raise NotImplementedError(
                "Multipart upload is only supported for S3 backends. "
                "Use simple upload (/ingest/upload) instead."
            )

        try:
            response = self._s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
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
        ttl_seconds: int,
    ) -> List[Dict[str, Any]]:
        """
        Generate pre-signed URLs for all parts of a multipart upload (S3 only).

        Args:
            key: S3 object key
            upload_id: Multipart upload ID
            num_parts: Number of parts to generate URLs for
            ttl_seconds: URL expiration time in seconds

        Returns:
            List of dicts with part_number and url

        Raises:
            NotImplementedError: If backend is not S3
        """
        if not self._is_s3:
            raise NotImplementedError(
                "Presigned URLs are only supported for S3 backends."
            )

        urls = []
        for part_number in range(1, num_parts + 1):
            url = self._s3_client.generate_presigned_url(
                'upload_part',
                Params={
                    'Bucket': self.bucket_name,
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
        Complete a multipart upload (S3 only).

        Args:
            key: S3 object key
            upload_id: Multipart upload ID
            parts: List of dicts with PartNumber and ETag

        Returns:
            Object ETag

        Raises:
            NotImplementedError: If backend is not S3
        """
        if not self._is_s3:
            raise NotImplementedError(
                "Multipart upload is only supported for S3 backends."
            )

        try:
            response = self._s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
            )
            logger.info(f"Completed multipart upload for {key}")
            return response['ETag']
        except ClientError as e:
            logger.error(f"Failed to complete multipart upload for {key}: {e}")
            raise

    # =========================================================================
    # URI Management
    # =========================================================================

    def get_object_url(self, key: str) -> str:
        """
        Get the storage URI for an object.

        Args:
            key: Storage key

        Returns:
            Storage URI (s3://bucket/key for S3, store://key for others)
        """
        if self._is_s3:
            return f"s3://{self.bucket_name}/{key}"
        else:
            return f"store://{key}"

    # =========================================================================
    # Capability Detection
    # =========================================================================

    def supports_multipart_upload(self) -> bool:
        """
        Check if backend supports multipart uploads.

        Returns:
            True if S3 backend, False otherwise
        """
        return self._is_s3

    def get_backend_type(self) -> str:
        """
        Get the backend type name.

        Returns:
            Backend class name (e.g., "BucketStore", "FilesystemStore")
        """
        return type(self.store).__name__
