import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from crowdgit.services import (
    CloneService,
    CommitService,
    MaintainerService,
    QueueService,
    SoftwareValueService,
    VulnerabilityScannerService,
)
from crowdgit.settings import WORKER_SHUTDOWN_TIMEOUT_SEC
from crowdgit.worker.repository_worker import RepositoryWorker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan context manager"""
    logger.info("Starting application lifespan")

    clone_service = CloneService()
    queue_service = QueueService()
    commit_service = CommitService(queue_service=queue_service)
    software_value_service = SoftwareValueService()
    vulnerability_scanner_service = VulnerabilityScannerService()
    maintainer_service = MaintainerService()

    worker_task = None
    worker = RepositoryWorker(
        clone_service=clone_service,
        commit_service=commit_service,
        software_value_service=software_value_service,
        vulnerability_scanner_service=vulnerability_scanner_service,
        maintainer_service=maintainer_service,
        queue_service=queue_service,
    )
    logger.info("Repo worker initialized")
    try:
        worker_task = asyncio.create_task(worker.run())
        logger.info("RepositoryWorker started successfully")
        # Yield control to FastAPI - it will handle signals and trigger shutdown
        yield
    finally:
        await worker.shutdown()  # stopping worker to process new repos
        try:
            await asyncio.wait_for(worker_task, timeout=WORKER_SHUTDOWN_TIMEOUT_SEC)
            logger.info("Worker shutdown complete")
        except asyncio.TimeoutError:
            logger.warning("Worker shutdown timeout, forcing cancellation")
            worker_task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Git Integration Worker Running", "status": "healthy"}
