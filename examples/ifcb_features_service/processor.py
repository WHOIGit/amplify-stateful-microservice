"""IFCB features extraction processor using ifcb-features library."""

import io
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
from PIL import Image
from ifcb import DataDirectory
from ifcb_features.all import compute_features
from pydantic import BaseModel

from stateful_microservice import BaseProcessor
from stateful_microservice.processor import JobInput
from stateful_microservice.output_writers import (
    JsonlResultUploader,
    WebDatasetUploader,
    write_results_index,
)

logger = logging.getLogger(__name__)


class FeatureRecord(BaseModel):
    """Schema for a single ROI feature response."""

    roi_number: int
    metrics: Dict[str, float]


class FeaturesJobResult(BaseModel):
    """Job-level payload emitted once processing completes."""

    outputs: List[Dict[str, Any]]
    metrics: Dict[str, int]
    results_index_uri: str


class FeaturesProcessor(BaseProcessor):
    """
    IFCB features extraction using the ifcb-features library.

    This processor extracts morphological features from IFCB ROI images
    and returns blob segmentation masks.
    """

    @property
    def name(self) -> str:
        return "features"

    @property
    def version(self) -> str:
        return "1.0.0"

    result_model = FeaturesJobResult

    def process_input(
        self,
        job_input: JobInput,
    ) -> FeaturesJobResult:
        """
        Extract features and masks from an IFCB input.

        Args:
            job_input: Runtime job input containing the job ID and local file paths

        Returns:
            FeaturesJobResult describing the uploaded outputs.

        Raises:
            ValueError: If required files are missing or no features extracted
        """
        # Validate required files
        input_label = self._infer_input_label(job_input.local_paths)
        files_by_ext = {}
        for raw_path in job_input.local_paths:
            path = Path(raw_path)
            if path.is_file():
                files_by_ext[path.suffix.lower()] = path

        missing = {'.adc', '.roi', '.hdr'} - set(files_by_ext.keys())
        if missing:
            raise ValueError(
                f"Missing required files for {input_label}: {missing}. "
                f"Available: {set(files_by_ext.keys())}"
            )

        # Create temporary directory with proper naming convention
        temp_dir = tempfile.mkdtemp()

        try:
            # Copy files with expected naming: {bin_id}.{ext}
            for ext, path in files_by_ext.items():
                dest = Path(temp_dir) / f"{input_label}{ext}"
                shutil.copy(path, dest)
                logger.debug(f"Copied {path} to {dest}")

            # Open with pyifcb DataDirectory
            data_dir = DataDirectory(temp_dir)
            bins = list(data_dir)

            if not bins:
                raise ValueError(f"No bins found in directory for {input_label}")

            sample = bins[0]

            # Process each ROI image
            features_list: List[FeatureRecord] = []
            masks_list: List[bytes] = []
            try:
                total_rois = len(sample.images)
            except TypeError:
                total_rois = getattr(sample, "n_rois", None)
            stride = max(1, total_rois // 20) if total_rois else 1

            self.report_progress(
                stage="processing",
                percent=0.0,
                message="processor_start",
                total_rois=total_rois,
            )

            for roi_index, (roi_number, image) in enumerate(sample.images.items(), start=1):
                try:
                    # compute_features returns (blobs_image, features)
                    # where features is a list of (name, value) tuples
                    blobs_image, roi_features = compute_features(image)

                    feature_record = FeatureRecord(
                        roi_number=roi_number,
                        metrics=dict(roi_features),
                    )
                    features_list.append(feature_record)

                    masks_list.append(self._encode_mask(blobs_image))

                except Exception as e:
                    logger.warning(f"Failed to process ROI {roi_number} in input {input_label}: {e}")
                    # Continue processing other ROIs
                    continue

                emit_update = False
                if total_rois:
                    if roi_index % stride == 0 or roi_index == total_rois:
                        emit_update = True
                else:
                    if roi_index % stride == 0:
                        emit_update = True

                if emit_update:
                    percent_complete = None
                    if total_rois:
                        percent_complete = (roi_index / total_rois) * 100.0

                    self.report_progress(
                        stage="processing",
                        percent=percent_complete,
                        roi_number=roi_number,
                        processed_rois=roi_index,
                        total_rois=total_rois,
                    )

            if not features_list:
                raise ValueError(f"No features extracted from input {input_label}")

            logger.info(
                f"Extracted {len(features_list)} features and {len(masks_list)} masks "
                f"from input {input_label}"
            )

            self.report_progress(
                stage="processing",
                percent=100.0,
                message="processor_complete",
                total_rois=total_rois,
                processed_rois=len(features_list),
            )

            responses_uploader = JsonlResultUploader(job_input.job_id, response_model=FeatureRecord)
            for record in features_list:
                responses_uploader.add_record(record)
            responses_output = responses_uploader.finalize()

            artifacts_uploader = WebDatasetUploader(job_input.job_id)
            for idx, mask in enumerate(masks_list):
                artifacts_uploader.add_artifact(input_label, f"{idx:05d}", mask, extension=".png")
            artifacts_output = artifacts_uploader.finalize()

            outputs: List[Dict[str, Any]] = []
            if responses_output["uris"]:
                responses_output["name"] = "records"
                outputs.append(responses_output)
            if artifacts_output:
                outputs.append(artifacts_output)

            metrics_payload = {
                "inputs_processed": 1,
                "records_emitted": len(features_list),
                "artifacts_emitted": len(masks_list),
            }

            results_uri = write_results_index(job_input.job_id, outputs, metrics_payload)

            return FeaturesJobResult(
                outputs=outputs,
                metrics=metrics_payload,
                results_index_uri=results_uri,
            )

        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _encode_mask(self, mask: np.ndarray) -> bytes:
        """Encode a mask array as PNG bytes."""
        data = mask
        if data.dtype != np.uint8:
            data = (data * 255).astype(np.uint8)
        image = Image.fromarray(data)
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    def _infer_input_label(self, local_paths: List[str]) -> str:
        """Best-effort guess at the input label based on filenames."""
        for raw_path in local_paths:
            stem = Path(raw_path).stem
            if stem:
                return stem
        return "input"
