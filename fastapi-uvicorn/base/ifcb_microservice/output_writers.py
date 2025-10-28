"""Output writers for features (Parquet) and masks (WebDataset TAR shards)."""

import io
import tarfile
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .storage import s3_client
from .config import settings

logger = logging.getLogger(__name__)


class ParquetFeaturesWriter:
    """Writer for features output in Parquet format."""

    def __init__(self, job_id: str):
        """
        Initialize Parquet writer.

        Args:
            job_id: Job ID for output path construction
        """
        self.job_id = job_id
        self.features_data: List[Dict[str, Any]] = []

    def add_bin_features(self, bin_id: str, features_df: pd.DataFrame):
        """
        Add features for a single bin.

        Args:
            bin_id: Bin identifier
            features_df: DataFrame with columns [roi_number, feature1, feature2, ...]
        """
        # Add bin_id column
        features_df = features_df.copy()
        features_df.insert(0, 'bin_id', bin_id)

        # Convert to records and append
        self.features_data.extend(features_df.to_dict('records'))
        logger.info(f"Added {len(features_df)} ROI features for bin {bin_id}")

    def write_to_s3(self) -> List[str]:
        """
        Write accumulated features to Parquet file(s) on S3.

        Returns:
            List of S3 URIs for created Parquet files
        """
        if not self.features_data:
            logger.warning("No features data to write")
            return []

        # Convert to DataFrame
        df = pd.DataFrame(self.features_data)

        # Define S3 path
        s3_key = f"{settings.s3_results_prefix}/{self.job_id}/features/part-0000.parquet"

        logger.info(f"Writing {len(df)} rows to {s3_key}")

        # Write to in-memory buffer as Parquet
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)

        # Upload to S3
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, s3_key)

        uri = s3_client.get_object_url(s3_key)
        logger.info(f"Successfully wrote features to {uri}")

        return [uri]

    def get_schema(self) -> Dict[str, str]:
        """
        Get the schema (column names and types).

        Returns:
            Dict mapping column names to type strings
        """
        if not self.features_data:
            return {}

        df = pd.DataFrame(self.features_data)
        schema = {}
        for col, dtype in df.dtypes.items():
            # Convert numpy dtypes to string representation
            if dtype.name.startswith('int'):
                schema[col] = 'int64'
            elif dtype.name.startswith('float'):
                schema[col] = 'float64'
            else:
                schema[col] = 'string'

        return schema


class WebDatasetMasksWriter:
    """Writer for masks output in WebDataset TAR shard format."""

    def __init__(self, job_id: str):
        """
        Initialize WebDataset TAR writer.

        Args:
            job_id: Job ID for output path construction
        """
        self.job_id = job_id
        self.current_shard_index = 0
        self.current_shard_size = 0
        self.current_shard_items: List[Dict[str, Any]] = []
        self.shards_info: List[Dict[str, str]] = []
        self.target_shard_size = settings.masks_shard_size_mb * 1024 * 1024

    def add_mask(self, bin_id: str, roi_number: int, mask_png_bytes: bytes):
        """
        Add a mask to the current shard.

        Args:
            bin_id: Bin identifier
            roi_number: ROI number
            mask_png_bytes: PNG-encoded mask bytes
        """
        mask_id = f"{bin_id}_{roi_number:05d}"

        self.current_shard_items.append({
            'bin_id': bin_id,
            'roi_number': roi_number,
            'mask_id': mask_id,
            'data': mask_png_bytes,
        })

        self.current_shard_size += len(mask_png_bytes)

        # Check if we should finalize this shard
        if self.current_shard_size >= self.target_shard_size:
            self._finalize_current_shard()

    def _finalize_current_shard(self):
        """Finalize and upload the current shard to S3."""
        if not self.current_shard_items:
            return

        shard_name = f"shard-{self.current_shard_index:05d}"
        tar_key = f"{settings.s3_results_prefix}/{self.job_id}/masks/{shard_name}.tar"
        index_key = f"{settings.s3_results_prefix}/{self.job_id}/masks/{shard_name}.json"

        logger.info(f"Finalizing shard {shard_name} with {len(self.current_shard_items)} masks")

        # Create TAR archive in memory
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
            for item in self.current_shard_items:
                mask_id = item['mask_id']
                data = item['data']

                # Create TarInfo for this file
                tarinfo = tarfile.TarInfo(name=f"{mask_id}.png")
                tarinfo.size = len(data)

                # Add to tar
                tar.addfile(tarinfo, io.BytesIO(data))

        # Upload TAR to S3
        tar_buffer.seek(0)
        s3_client.upload_fileobj(tar_buffer, tar_key)

        # Create index (maps mask_id to bin_id and roi_number)
        index_data = {
            item['mask_id']: {
                'bin_id': item['bin_id'],
                'roi_number': item['roi_number'],
                'tar_member': f"{item['mask_id']}.png",
            }
            for item in self.current_shard_items
        }

        # Upload index to S3
        index_buffer = io.BytesIO(json.dumps(index_data, indent=2).encode('utf-8'))
        s3_client.upload_fileobj(index_buffer, index_key)

        # Record shard info
        self.shards_info.append({
            'uri': s3_client.get_object_url(tar_key),
            'index_uri': s3_client.get_object_url(index_key),
        })

        logger.info(f"Uploaded shard {shard_name} ({self.current_shard_size} bytes)")

        # Reset for next shard
        self.current_shard_index += 1
        self.current_shard_size = 0
        self.current_shard_items = []

    def finalize(self) -> List[Dict[str, str]]:
        """
        Finalize any remaining masks and return shard information.

        Returns:
            List of shard info dicts with 'uri' and 'index_uri'
        """
        # Finalize any remaining items
        self._finalize_current_shard()

        logger.info(f"Finalized {len(self.shards_info)} shards for job {self.job_id}")
        return self.shards_info


class ResultsWriter:
    """Coordinator for writing both features and masks."""

    def __init__(self, job_id: str):
        """
        Initialize results writer.

        Args:
            job_id: Job ID
        """
        self.job_id = job_id
        self.features_writer = ParquetFeaturesWriter(job_id)
        self.masks_writer = WebDatasetMasksWriter(job_id)
        self.total_bins = 0
        self.total_rois = 0
        self.total_masks = 0

    def add_bin_results(self, bin_id: str, features_df: pd.DataFrame, masks: List[bytes]):
        """
        Add results for a single bin.

        Args:
            bin_id: Bin identifier
            features_df: Features DataFrame
            masks: List of PNG-encoded mask bytes (one per ROI)
        """
        # Add features
        self.features_writer.add_bin_features(bin_id, features_df)

        # Add masks
        for i, mask_bytes in enumerate(masks):
            roi_number = int(features_df.iloc[i]['roi_number'])
            self.masks_writer.add_mask(bin_id, roi_number, mask_bytes)

        self.total_bins += 1
        self.total_rois += len(features_df)
        self.total_masks += len(masks)

        logger.info(f"Added results for bin {bin_id}: {len(features_df)} ROIs, {len(masks)} masks")

    def finalize(self) -> Dict[str, Any]:
        """
        Finalize all outputs and create results index.

        Returns:
            Results index dict
        """
        # Write features to S3
        feature_uris = self.features_writer.write_to_s3()
        feature_schema = self.features_writer.get_schema()

        # Finalize masks
        mask_shards = self.masks_writer.finalize()

        # Create results index
        results = {
            'job_id': self.job_id,
            'features': {
                'format': 'parquet',
                'uris': feature_uris,
                'schema': feature_schema,
            },
            'masks': {
                'format': 'webdataset',
                'shards': mask_shards,
            },
            'counts': {
                'bins': self.total_bins,
                'rois': self.total_rois,
                'masks': self.total_masks,
            }
        }

        # Upload results index to S3
        results_key = f"{settings.s3_results_prefix}/{self.job_id}/results.json"
        results_buffer = io.BytesIO(json.dumps(results, indent=2).encode('utf-8'))
        s3_client.upload_fileobj(results_buffer, results_key)

        logger.info(f"Uploaded results index to {results_key}")

        return results
