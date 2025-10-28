"""Ingest service for handling multipart uploads."""

import math
import uuid
from datetime import datetime, timedelta
from typing import List, Dict
import logging

from .storage import s3_client
from .jobs import job_store
from .config import settings
from .models import (
    IngestStartRequest,
    IngestStartResponse,
    FileUploadInfo,
    PartUrl,
    IngestCompleteRequest,
    IngestCompleteResponse,
)

logger = logging.getLogger(__name__)


class IngestService:
    """Service for managing multipart upload ingestion."""

    def start_ingest(self, request: IngestStartRequest) -> IngestStartResponse:
        """
        Start ingestion for a bin (initiate multipart uploads for 3 files).

        Args:
            request: Ingest start request with bin_id and file specs

        Returns:
            Response with pre-signed URLs for all parts
        """
        job_id = str(uuid.uuid4())
        bin_id = request.bin_id
        files_info: List[FileUploadInfo] = []

        logger.info(f"Starting ingest for bin {bin_id}, job {job_id}")

        for file_spec in request.files:
            file_id = str(uuid.uuid4())
            filename = file_spec.filename
            size_bytes = file_spec.size_bytes

            # Calculate S3 key: datasets/{job_id}/{bin_id}/{filename}
            s3_key = f"{settings.s3_datasets_prefix}/{job_id}/{bin_id}/{filename}"

            # Calculate number of parts needed
            part_size = settings.multipart_part_size_mb * 1024 * 1024
            num_parts = math.ceil(size_bytes / part_size)

            logger.info(f"File {filename}: {size_bytes} bytes, {num_parts} parts")

            # Create multipart upload
            upload_id = s3_client.create_multipart_upload(s3_key)

            # Generate pre-signed URLs for all parts
            part_urls = s3_client.generate_presigned_part_urls(
                key=s3_key,
                upload_id=upload_id,
                num_parts=num_parts,
            )

            # Store upload metadata for later completion
            job_store.store_upload_info(
                job_id=job_id,
                file_id=file_id,
                upload_info={
                    'filename': filename,
                    's3_key': s3_key,
                    'upload_id': upload_id,
                    'size_bytes': size_bytes,
                    'num_parts': num_parts,
                    'completed': False,
                }
            )

            files_info.append(
                FileUploadInfo(
                    file_id=file_id,
                    filename=filename,
                    s3_key=s3_key,
                    upload_id=upload_id,
                    part_urls=[PartUrl(**url) for url in part_urls],
                )
            )

        # Calculate expiration time
        expires_at = datetime.utcnow() + timedelta(seconds=settings.multipart_url_ttl_seconds)

        # Store job metadata
        job_store._upload_metadata[job_id] = {
            'bin_id': bin_id,
            'uploads': {
                info.file_id: {
                    'filename': info.filename,
                    's3_key': info.s3_key,
                    'upload_id': info.upload_id,
                    'completed': False,
                }
                for info in files_info
            }
        }

        return IngestStartResponse(
            job_id=job_id,
            bin_id=bin_id,
            files=files_info,
            expires_at=expires_at,
        )

    def complete_ingest(self, request: IngestCompleteRequest) -> IngestCompleteResponse:
        """
        Complete multipart upload for a file.

        Args:
            request: Complete request with parts and ETags

        Returns:
            Response with final object information
        """
        job_id = request.job_id
        file_id = request.file_id
        upload_id = request.upload_id

        # Get upload metadata
        upload_info = job_store.get_upload_info(job_id, file_id)
        if not upload_info:
            raise ValueError(f"Upload info not found for job {job_id}, file {file_id}")

        # Validate upload_id matches
        if upload_info['upload_id'] != upload_id:
            raise ValueError(f"Upload ID mismatch for file {file_id}")

        s3_key = upload_info['s3_key']

        logger.info(f"Completing multipart upload for {s3_key}")

        # Convert parts to S3 format
        parts = [
            {'PartNumber': part.PartNumber, 'ETag': part.ETag}
            for part in request.parts
        ]

        # Complete the multipart upload
        etag = s3_client.complete_multipart_upload(
            key=s3_key,
            upload_id=upload_id,
            parts=parts,
        )

        # Mark as completed
        upload_info['completed'] = True
        upload_info['etag'] = etag

        return IngestCompleteResponse(
            file_id=file_id,
            s3_key=s3_key,
            etag=etag,
            status="completed",
        )

    def check_bin_ready(self, job_id: str) -> bool:
        """
        Check if all 3 files for a bin have been uploaded.

        Args:
            job_id: Job ID

        Returns:
            True if all files completed, False otherwise
        """
        metadata = job_store.get_job_metadata(job_id)
        if not metadata:
            return False

        uploads = metadata.get('uploads', {})
        if len(uploads) != 3:
            return False

        return all(upload.get('completed', False) for upload in uploads.values())

    def get_bin_manifest_entry(self, job_id: str) -> Dict:
        """
        Generate a manifest entry for a completed bin.

        Args:
            job_id: Job ID

        Returns:
            Manifest entry dict
        """
        metadata = job_store.get_job_metadata(job_id)
        if not metadata:
            raise ValueError(f"Job {job_id} not found")

        bin_id = metadata['bin_id']
        uploads = metadata.get('uploads', {})

        # Get S3 URIs for all files
        files = [
            s3_client.get_object_url(upload['s3_key'])
            for upload in uploads.values()
        ]

        # Calculate total size
        total_bytes = sum(upload.get('size_bytes', 0) for upload in uploads.values())

        return {
            'bin_id': bin_id,
            'files': files,
            'bytes': total_bytes,
        }


# Global ingest service instance
ingest_service = IngestService()
