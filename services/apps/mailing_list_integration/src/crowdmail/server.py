import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from crowdmail.settings import WORKER_SHUTDOWN_TIMEOUT_SEC
from crowdmail.worker.list_worker import ListWorker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan context manager"""
    logger.info("Starting application lifespan")

    worker_task = None
    worker = ListWorker()
    logger.info("List worker initialized")
    try:
        worker_task = asyncio.create_task(worker.run())
        logger.info("ListWorker started successfully")
        # Yield control to FastAPI - it will handle signals and trigger shutdown
        yield
    finally:
        await worker.shutdown()  # stopping worker to process new lists
        if worker_task is not None:
            try:
                await asyncio.wait_for(worker_task, timeout=WORKER_SHUTDOWN_TIMEOUT_SEC)
                logger.info("Worker shutdown complete")
            except TimeoutError:
                logger.warning("Worker shutdown timeout, forcing cancellation")
                worker_task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Mailing List Integration Worker Running", "status": "healthy"}
