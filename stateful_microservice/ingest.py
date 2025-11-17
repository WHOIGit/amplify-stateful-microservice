"""Ingest service for handling multipart uploads."""

import math
import uuid
from datetime import datetime, timedelta
from typing import List
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
        Start ingestion for files (initiate multipart uploads).

        Args:
            request: Ingest start request with file specifications

        Returns:
            Response with pre-signed URLs for all parts
        """
        if not request.files:
            raise ValueError("At least one file must be provided")

        job_id = str(uuid.uuid4())
        logger.info(f"Starting ingest for {len(request.files)} file(s), job {job_id}")

        files_info: List[FileUploadInfo] = []

        for file_spec in request.files:
            file_id = str(uuid.uuid4())
            filename = file_spec.filename
            size_bytes = file_spec.size_bytes
            s3_key = f"{settings.s3_datasets_prefix}/{job_id}/{filename}"

            part_size = settings.multipart_part_size_mb * 1024 * 1024
            num_parts = max(1, math.ceil(size_bytes / part_size))

            logger.info(
                f"Job {job_id}: preparing {filename} "
                f"({size_bytes} bytes, {num_parts} part(s))"
            )

            upload_id = s3_client.create_multipart_upload(s3_key)

            part_urls = s3_client.generate_presigned_part_urls(
                key=s3_key,
                upload_id=upload_id,
                num_parts=num_parts,
            )

            job_store.store_upload_info(
                job_id=job_id,
                file_id=file_id,
                upload_info={
                    's3_key': s3_key,
                    'upload_id': upload_id,
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

        expires_at = datetime.utcnow() + timedelta(seconds=settings.multipart_url_ttl_seconds)

        return IngestStartResponse(
            job_id=job_id,
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

        logger.info(f"Completing multipart upload for job {job_id} file {s3_key}")

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

        job_store.mark_upload_completed(job_id, file_id, etag)

        logger.info(f"Completed multipart upload for job {job_id} file {file_id}")

        # If all files are complete, create the job
        if job_store.all_files_complete(job_id):
            file_uris = job_store.get_completed_file_uris(job_id)
            manifest_data = {'files': file_uris}
            job_store.set_manifest_data(job_id, manifest_data)

            if not job_store.get_job(job_id):
                job_store.create_job(
                    manifest_data=manifest_data,
                    job_id_override=job_id,
                )
                logger.info(f"All files ready for job {job_id}; job queued for processing")

        return IngestCompleteResponse(
            file_id=file_id,
            s3_key=s3_key,
            etag=etag,
            status="completed",
        )


# Global ingest service instance
ingest_service = IngestService()
