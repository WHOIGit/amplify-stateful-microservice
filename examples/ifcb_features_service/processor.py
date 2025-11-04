"""IFCB features extraction processor using ifcb-features library."""

from pathlib import Path
from typing import Dict, Tuple, List
import tempfile
import shutil
import logging

import pandas as pd
import numpy as np
from ifcb import DataDirectory
from ifcb_features.all import compute_features

from ifcb_microservice import BaseProcessor

logger = logging.getLogger(__name__)


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

    def process_bin(
        self,
        bin_id: str,
        bin_files: Dict[str, Path]
    ) -> Tuple[pd.DataFrame, List[np.ndarray]]:
        """
        Extract features and masks from an IFCB bin.

        Args:
            bin_id: Bin identifier
            bin_files: Dict mapping file extension to local path

        Returns:
            Tuple of:
                - DataFrame with ~241 feature columns per ROI
                - List of blob mask images (numpy arrays)

        Raises:
            ValueError: If required files are missing or no features extracted
        """
        # Validate required files
        self.validate_bin_files(bin_files, {'.adc', '.roi', '.hdr'})

        # Create temporary directory with proper naming convention
        temp_dir = tempfile.mkdtemp()

        try:
            # Copy files with expected naming: {bin_id}.{ext}
            for ext, path in bin_files.items():
                dest = Path(temp_dir) / f"{bin_id}{ext}"
                shutil.copy(path, dest)
                logger.debug(f"Copied {path} to {dest}")

            # Open with pyifcb DataDirectory
            data_dir = DataDirectory(temp_dir)
            bins = list(data_dir)

            if not bins:
                raise ValueError(f"No bins found in directory for {bin_id}")

            sample = bins[0]

            # Process each ROI image
            features_list = []
            masks_list = []
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

                    # Convert features to dict
                    feature_dict = {'roi_number': roi_number}
                    feature_dict.update(dict(roi_features))
                    features_list.append(feature_dict)

                    # Store blob mask
                    masks_list.append(blobs_image)

                except Exception as e:
                    logger.warning(f"Failed to process ROI {roi_number} in bin {bin_id}: {e}")
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
                raise ValueError(f"No features extracted from bin {bin_id}")

            # Convert to DataFrame
            features_df = pd.DataFrame(features_list)

            # Validate output
            self.validate_output(features_df)

            logger.info(
                f"Extracted {len(features_df)} features and {len(masks_list)} masks "
                f"from bin {bin_id}"
            )

            self.report_progress(
                stage="processing",
                percent=100.0,
                message="processor_complete",
                total_rois=total_rois,
                processed_rois=len(features_df),
            )

            return features_df, masks_list

        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)

    def get_output_schema(self) -> Dict[str, str]:
        """
        Define expected output schema.

        The ifcb-features library produces ~241 feature columns.
        Key columns include morphology, texture, and geometry features.
        """
        return {
            'roi_number': 'int64',
            'Area': 'float64',
            'Biovolume': 'float64',
            'Eccentricity': 'float64',
            'MajorAxisLength': 'float64',
            'MinorAxisLength': 'float64',
            # ... and ~236 more feature columns from ifcb-features
        }
