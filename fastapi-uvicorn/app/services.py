"""Service layer wrapping ifcb-features functionality."""

from typing import Dict, Any, List
import tempfile
import shutil
import zipfile
import os
from pathlib import Path

from extract_slim_features import extract_and_save_all_features
import pandas as pd


class FeatureExtractionService:
    """Service for IFCB feature extraction operations."""

    @staticmethod
    def process_bins_from_zip(zip_file_path: str) -> str:
        """
        Process multiple IFCB bins from a ZIP archive and create result ZIP.

        Args:
            zip_file_path: Path to the ZIP file containing bin files

        Returns:
            Path to the result ZIP file containing features.json and blob ZIPs

        Raises:
            ValueError: If ZIP extraction or processing fails
        """
        input_temp_dir = None
        output_temp_dir = None
        result_zip_path = None

        try:
            # Create temporary directories
            input_temp_dir = tempfile.mkdtemp(prefix="ifcb_input_")
            output_temp_dir = tempfile.mkdtemp(prefix="ifcb_output_")

            # Extract ZIP to input temp directory
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(input_temp_dir)

            # Call extract_slim_features to process all bins
            extract_and_save_all_features(input_temp_dir, output_temp_dir)

            # Read generated CSV files and parse results
            results = []
            for csv_file in Path(output_temp_dir).glob("*_features_v4.csv"):
                # Extract bin_id from filename (e.g., D20150314T203456_IFCB102_features_v4.csv)
                bin_id = csv_file.stem.replace("_features_v4", "")

                # Read CSV
                df = pd.read_csv(csv_file)

                # Convert to list of ROI features
                rois = []
                for _, row in df.iterrows():
                    roi_number = int(row['roi_number'])
                    features = row.drop('roi_number').to_dict()
                    rois.append({
                        "roi_number": roi_number,
                        "features": features
                    })

                results.append({
                    "bin_id": bin_id,
                    "roi_count": len(rois),
                    "rois": rois
                })

            # Create result ZIP with features JSON and blob ZIPs
            result_zip_fd, result_zip_path = tempfile.mkstemp(suffix='.zip', prefix='ifcb_result_')
            os.close(result_zip_fd)

            with zipfile.ZipFile(result_zip_path, 'w', zipfile.ZIP_DEFLATED) as result_zip:
                # Add features as JSON
                import json
                features_json = json.dumps({
                    "bins_processed": len(results),
                    "total_rois": sum(bin_data["roi_count"] for bin_data in results),
                    "bins": results
                }, indent=2)
                result_zip.writestr("features.json", features_json)

                # Add all blob ZIPs
                for blob_zip_file in Path(output_temp_dir).glob("*_blobs_v4.zip"):
                    result_zip.write(blob_zip_file, arcname=blob_zip_file.name)

            return result_zip_path

        except zipfile.BadZipFile:
            raise ValueError("Invalid ZIP file")
        except Exception as e:
            raise ValueError(f"Bin processing failed: {str(e)}")
        finally:
            # Clean up temporary directories (but NOT result_zip_path - caller will clean that)
            if input_temp_dir and os.path.exists(input_temp_dir):
                shutil.rmtree(input_temp_dir)
            if output_temp_dir and os.path.exists(output_temp_dir):
                shutil.rmtree(output_temp_dir)
