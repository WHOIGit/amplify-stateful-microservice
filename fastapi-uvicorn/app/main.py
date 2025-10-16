"""FastAPI application for IFCB feature extraction microservice."""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
import logging
import tempfile
import os

from .models import (
    HealthResponse,
    ErrorResponse,
)
from .services import FeatureExtractionService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="IFCB Features Microservice",
    description="REST API for IFCB image segmentation and feature extraction",
    version="1.0.0",
)

# Initialize service
feature_service = FeatureExtractionService()


@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint - returns health status."""
    return HealthResponse(status="healthy", version="1.0.0")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(status="healthy", version="1.0.0")


@app.post(
    "/process-bins",
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
async def process_bins(file: UploadFile = File(...)):
    """
    Process multiple IFCB bins from a ZIP archive.

    Args:
        file: ZIP file containing IFCB bin files (.adc, .roi, .hdr)

    Returns:
        ZIP file containing features.json and blob mask ZIPs

    Raises:
        HTTPException: If bin processing fails
    """
    input_zip_path = None
    result_zip_path = None

    try:
        # Validate file is a ZIP
        if not file.filename.endswith('.zip'):
            raise HTTPException(status_code=400, detail="File must be a ZIP archive")

        logger.info(f"Processing uploaded ZIP file: {file.filename}")

        # Save uploaded file to temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
            input_zip_path = temp_zip.name
            content = await file.read()
            temp_zip.write(content)

        # Process bins and get result ZIP path
        logger.info("Extracting and processing bins")
        result_zip_path = feature_service.process_bins_from_zip(input_zip_path)

        logger.info(f"Processing complete, returning result ZIP")

        # Return ZIP file download
        return FileResponse(
            path=result_zip_path,
            media_type="application/zip",
            filename="ifcb_results.zip",
            background=lambda: os.remove(result_zip_path) if os.path.exists(result_zip_path) else None
        )

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        # Clean up result zip if it exists
        if result_zip_path and os.path.exists(result_zip_path):
            os.remove(result_zip_path)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Internal error: {str(e)}")
        # Clean up result zip if it exists
        if result_zip_path and os.path.exists(result_zip_path):
            os.remove(result_zip_path)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        # Clean up input ZIP file
        if input_zip_path and os.path.exists(input_zip_path):
            os.remove(input_zip_path)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
