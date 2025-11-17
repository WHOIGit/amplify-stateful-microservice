"""FastAPI application factory for long-running stateful microservices."""

from contextlib import asynccontextmanager
import logging
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException

from .processor import BaseProcessor
from .worker import create_worker_pool
from .models import (
    HealthResponse,
    ErrorResponse,
    IngestStartRequest,
    IngestStartResponse,
    IngestCompleteRequest,
    IngestCompleteResponse,
    JobSubmitRequest,
    JobSubmitResponse,
    JobStatus,
)
from .ingest import ingest_service
from .jobs import job_store

logger = logging.getLogger(__name__)


@dataclass
class ServiceConfig:
    """
    Configuration for building a microservice application.

    Args:
        name: Override service name (defaults to processor.name)
        version: Override service version (defaults to processor.version)
        description: Short description for generated docs
    """

    name: str | None = None
    version: str | None = None
    description: str | None = None


def create_app(processor: BaseProcessor, config: ServiceConfig | None = None) -> FastAPI:
    """
    Create a FastAPI application for a long-running processor.

    This factory function creates a complete microservice with all
    necessary endpoints (health, jobs, ingest) and background workers.

    Args:
        processor: Implementation of BaseProcessor for the algorithm
        config: Optional service configuration

    Returns:
        Configured FastAPI application

    Example:
        >>> from stateful_microservice import create_app, BaseProcessor, ServiceConfig, DefaultResult
        >>> class MyProcessor(BaseProcessor):
        ...     result_model = DefaultResult
        ...     def process_input(self, job_input):
        ...         # Algorithm implementation
        ...         return DefaultResult()
        >>> app = create_app(MyProcessor())
    """
    config = config or ServiceConfig()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Resolve metadata defaults from processor
    service_name = config.name or processor.name
    service_version = config.version or processor.version
    service_description = config.description or f"{service_name} processing service"

    # Create worker pool with processor
    worker_pool = create_worker_pool(processor)

    # Lifespan context manager for startup/shutdown
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Handle startup and shutdown events."""
        # Startup
        logger.info(f"Starting {service_name} Microservice v{service_version}")
        if worker_pool:
            await worker_pool.start()
            logger.info("Worker pool started")
        yield
        # Shutdown
        logger.info("Shutting down...")
        if worker_pool:
            await worker_pool.stop()
            logger.info("Shutdown complete")

    # Initialize FastAPI app
    app = FastAPI(
        title=f"{service_name.title()} Microservice",
        description=service_description,
        version=service_version,
        lifespan=lifespan,
    )

    # Expose helpful references on app state
    app.state.processor = processor
    app.state.service_config = config

    # ============================================================================
    # Health & Status Endpoints
    # ============================================================================

    @app.get("/", response_model=HealthResponse)
    async def root():
        """Root endpoint - returns health status."""
        return HealthResponse(
            status="healthy",
            version=service_version
        )

    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint."""
        return HealthResponse(
            status="healthy",
            version=service_version
        )

    # ============================================================================
    # Ingest Endpoints (Multipart Upload)
    # ============================================================================

    @app.post(
        "/ingest/start",
        response_model=IngestStartResponse,
        responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    )
    async def ingest_start(request: IngestStartRequest):
        """Start ingestion for files (initiate multipart uploads)."""
        try:
            response = ingest_service.start_ingest(request)
            logger.info(
                f"Started ingest for job {response.job_id} "
                f"with {len(response.files)} file(s)"
            )
            return response
        except Exception as e:
            logger.error(f"Failed to start ingest: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to start ingest: {str(e)}")

    @app.post(
        "/ingest/complete",
        response_model=IngestCompleteResponse,
        responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    )
    async def ingest_complete(request: IngestCompleteRequest):
        """Complete multipart upload for a file."""
        try:
            response = ingest_service.complete_ingest(request)
            logger.info(
                f"Completed upload for file {request.file_id}, job {request.job_id}"
            )
            return response
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to complete ingest: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to complete ingest: {str(e)}")

    # ============================================================================
    # Job Endpoints
    # ============================================================================

    @app.post(
        "/jobs",
        response_model=JobSubmitResponse,
        responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    )
    async def submit_job(request: JobSubmitRequest):
        """Submit a job for processing."""
        try:
            if not request.manifest_uri and not request.manifest_inline:
                raise HTTPException(
                    status_code=400,
                    detail="Either manifest_uri or manifest_inline must be provided"
                )

            manifest_data = request.manifest_inline.model_dump() if request.manifest_inline else None
            job_id = job_store.create_job(
                manifest_uri=request.manifest_uri,
                manifest_data=manifest_data,
                parameters=request.parameters.model_dump() if request.parameters else {},
            )

            job = job_store.get_job(job_id)
            logger.info(f"Created job {job_id}")

            return JobSubmitResponse(
                job_id=job_id,
                status=job.status,
                created_at=job.created_at,
            )
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to submit job: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to submit job: {str(e)}")

    @app.get(
        "/jobs/{job_id}",
        response_model=JobStatus,
        responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    )
    async def get_job_status(job_id: str):
        """Get the status of a job."""
        job = job_store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        return job

    @app.get(
        "/jobs",
        response_model=list[JobStatus],
        responses={500: {"model": ErrorResponse}},
    )
    async def list_jobs(limit: int = 50):
        """List recent jobs."""
        try:
            jobs = job_store.list_jobs(limit=min(limit, 100))
            return jobs
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")

    return app
