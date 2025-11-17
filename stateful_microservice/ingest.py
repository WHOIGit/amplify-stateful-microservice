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
    InputUploadInfo,
    PartUrl,
    IngestCompleteRequest,
    IngestCompleteResponse,
)

logger = logging.getLogger(__name__)


class IngestService:
    """Service for managing multipart upload ingestion."""

    def start_ingest(self, request: IngestStartRequest) -> IngestStartResponse:
        """
        Start ingestion for one or more inputs (initiate multipart uploads for each file).

        Args:
            request: Ingest start request describing the inputs/files

        Returns:
            Response with pre-signed URLs for all parts
        """
        if not request.inputs:
            raise ValueError("At least one input must be provided")

        input_ids = [input_spec.input_id for input_spec in request.inputs]
        if len(input_ids) != len(set(input_ids)):
            raise ValueError("Duplicate input IDs are not allowed within a single ingest request")

        job_id = str(uuid.uuid4())
        logger.info(f"Starting ingest for {len(request.inputs)} input(s), job {job_id}")

        job_store.init_ingest_job(job_id, input_ids)

        input_infos: List[InputUploadInfo] = []

        for input_spec in request.inputs:
            files_info: List[FileUploadInfo] = []
            input_id = input_spec.input_id

            for file_spec in input_spec.files:
                file_id = str(uuid.uuid4())
                filename = file_spec.filename
                size_bytes = file_spec.size_bytes
                s3_key = f"{settings.s3_datasets_prefix}/{job_id}/{input_id}/{filename}"

                part_size = settings.multipart_part_size_mb * 1024 * 1024
                num_parts = max(1, math.ceil(size_bytes / part_size))

                logger.info(
                    f"Job {job_id} input {input_id}: preparing {filename} "
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
                    input_id=input_id,
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

            input_infos.append(
                InputUploadInfo(
                    input_id=input_id,
                    files=files_info,
                )
            )

        expires_at = datetime.utcnow() + timedelta(seconds=settings.multipart_url_ttl_seconds)

        return IngestStartResponse(
            job_id=job_id,
            inputs=input_infos,
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
        input_id = request.input_id
        file_id = request.file_id
        upload_id = request.upload_id

        # Get upload metadata
        upload_info = job_store.get_upload_info(job_id, input_id, file_id)
        if not upload_info:
            raise ValueError(f"Upload info not found for job {job_id}, input {input_id}, file {file_id}")

        # Validate upload_id matches
        if upload_info['upload_id'] != upload_id:
            raise ValueError(f"Upload ID mismatch for file {file_id}")

        s3_key = upload_info['s3_key']

        logger.info(f"Completing multipart upload for job {job_id} input {input_id} file {s3_key}")

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

        job_store.mark_upload_completed(job_id, input_id, file_id, etag)

        logger.info(f"Completed multipart upload for job {job_id} input {input_id} file {file_id}")

        # If the entire input is ready, build manifest entry
        if job_store.input_is_complete(job_id, input_id) and not job_store.input_manifest_exists(job_id, input_id):
            manifest_entry = self._build_manifest_entry(job_id, input_id)
            job_store.set_input_manifest(job_id, input_id, manifest_entry)
            logger.info(f"Input {input_id} for job {job_id} ready for processing")

        # If all inputs are ready, create job if not already present
        if job_store.all_inputs_have_manifest(job_id):
            manifest_entries = job_store.get_manifest_entries(job_id)
            manifest_data = {'inputs': manifest_entries}
            job_store.set_manifest_data(job_id, manifest_data)

            if not job_store.get_job(job_id):
                job_store.create_job(
                    manifest_data=manifest_data,
                    parameters={},
                    idempotency_key=None,
                    job_id_override=job_id,
                )
                logger.info(f"All inputs ready for job {job_id}; job queued for processing")

        return IngestCompleteResponse(
            file_id=file_id,
            s3_key=s3_key,
            etag=etag,
            status="completed",
        )

    def _build_manifest_entry(self, job_id: str, input_id: str) -> Dict:
        """Construct a manifest entry for a completed input."""
        uploads = job_store.get_input_uploads(job_id, input_id)
        if not uploads:
            raise ValueError(f"No uploads recorded for job {job_id}, input {input_id}")

        file_order = job_store.get_input_file_order(job_id, input_id)
        if not file_order:
            file_order = list(uploads.keys())

        files = []
        for file_id in file_order:
            info = uploads.get(file_id)
            if not info:
                continue
            files.append(s3_client.get_object_url(info['s3_key']))

        return {
            'input_id': input_id,
            'files': files,
        }


# Global ingest service instance
ingest_service = IngestService()
