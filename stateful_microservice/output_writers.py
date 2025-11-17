"""Optional helpers for processors to upload structured results and artifacts."""

import io
import json
import tarfile
import logging
from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel

from .config import settings
from .storage import s3_client

logger = logging.getLogger(__name__)


class JsonlResultUploader:
    """
    Helper for processors to stream JSONL records to S3.

    Example:
        uploader = JsonlResultUploader(job_id, response_model=FeatureRecord)
        for record in records:
            uploader.add_record(record)
        response_output = uploader.finalize()
    """

    def __init__(self, job_id: str, response_model: Optional[type[BaseModel]] = None):
        self.job_id = job_id
        self.response_model = response_model
        self.buffer = io.BytesIO()
        self.max_file_size = settings.responses_chunk_size_mb * 1024 * 1024
        self.file_index = 0
        self.total_records = 0
        self.uris: List[str] = []

    def add_record(self, record: Dict[str, Any] | BaseModel):
        """Add a single record to the JSONL stream."""
        if isinstance(record, BaseModel):
            data = record.model_dump()
        else:
            data = dict(record)

        if self.response_model:
            data = self.response_model(**data).model_dump()

        encoded = (json.dumps(data) + "\n").encode("utf-8")
        if self.buffer.tell() + len(encoded) > self.max_file_size:
            self._flush()

        self.buffer.write(encoded)
        self.total_records += 1

    def _flush(self):
        if self.buffer.tell() == 0:
            return

        self.buffer.seek(0)
        key = f"{settings.s3_results_prefix}/{self.job_id}/responses_{self.file_index:05d}.jsonl"
        s3_client.upload_fileobj(self.buffer, key)
        self.uris.append(s3_client.get_object_url(key))

        logger.info(f"Uploaded JSONL chunk {key} ({self.total_records} records so far)")

        self.buffer = io.BytesIO()
        self.file_index += 1

    def finalize(self) -> Dict[str, Any]:
        """Flush remaining data and return output metadata."""
        self._flush()
        artifact = {
            "name": "responses",
            "type": "tabular",
            "format": "jsonl",
            "uris": self.uris,
            "shards": [],
            "metadata": {
                "records_emitted": self.total_records,
                "file_count": len(self.uris),
            },
        }
        if self.response_model:
            artifact["metadata"]["response_model"] = self.response_model.__name__
        return artifact


class WebDatasetUploader:
    """
    Helper for processors to build and upload WebDataset-style artifact shards.

    Example:
        uploader = WebDatasetUploader(job_id)
        uploader.add_artifact(input_id, record_id, png_bytes)
        shards_output = uploader.finalize()
    """

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.max_shard_size = settings.artifact_shard_size_mb * 1024 * 1024
        self.current_items: List[tuple[str, bytes]] = []
        self.current_size = 0
        self.shard_index = 0
        self.shards: List[Dict[str, Any]] = []

    def add_artifact(self, input_id: str, record_id: str | int, data: bytes, extension: str = ".bin"):
        """Add an artifact blob to the current shard."""
        if self.current_size + len(data) > self.max_shard_size:
            self._flush()

        name = f"{input_id}/{record_id}{extension}"
        self.current_items.append((name, data))
        self.current_size += len(data)

    def _flush(self):
        if not self.current_items:
            return

        shard_name = f"artifacts_{self.shard_index:05d}"
        tar_key = f"{settings.s3_results_prefix}/{self.job_id}/{shard_name}.tar"
        idx_key = f"{settings.s3_results_prefix}/{self.job_id}/{shard_name}.json"

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            for name, data in self.current_items:
                info = tarfile.TarInfo(name=name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))

        tar_buffer.seek(0)
        s3_client.upload_fileobj(tar_buffer, tar_key)
        shard_uri = s3_client.get_object_url(tar_key)

        index_payload = {
            "items": [
                {"name": name, "size_bytes": len(data)}
                for name, data in self.current_items
            ]
        }
        idx_buffer = io.BytesIO(json.dumps(index_payload, indent=2).encode("utf-8"))
        s3_client.upload_fileobj(idx_buffer, idx_key)
        index_uri = s3_client.get_object_url(idx_key)

        self.shards.append({
            "name": shard_name,
            "uri": shard_uri,
            "index_uri": index_uri,
            "metadata": {"items": len(self.current_items), "bytes": self.current_size},
        })

        logger.info(f"Uploaded artifact shard {shard_name} with {len(self.current_items)} items")

        self.current_items = []
        self.current_size = 0
        self.shard_index += 1

    def finalize(self) -> Dict[str, Any] | None:
        """Flush remaining artifacts and return shard metadata."""
        self._flush()
        if not self.shards:
            return None

        return {
            "name": "artifacts",
            "type": "binary",
            "format": "webdataset",
            "uris": [],
            "shards": self.shards,
            "metadata": {},
        }


def write_results_index(job_id: str, outputs: Sequence[Dict[str, Any]], metrics: Dict[str, Any]) -> str:
    """Write a consolidated results index JSON to S3 and return its URI."""
    payload = {
        "job_id": job_id,
        "outputs": list(outputs),
        "metrics": metrics,
    }

    key = f"{settings.s3_results_prefix}/{job_id}/results.json"
    buffer = io.BytesIO(json.dumps(payload, indent=2).encode("utf-8"))
    s3_client.upload_fileobj(buffer, key)

    uri = s3_client.get_object_url(key)
    logger.info(f"Wrote results index for job {job_id} to {uri}")
    return uri
