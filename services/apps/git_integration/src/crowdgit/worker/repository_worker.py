import asyncio

from crowdgit.database.crud import (
    acquire_repo_for_processing,
    get_recently_processed_repository_by_url,
    mark_repo_as_processed,
    release_repo,
    update_repository_licenses,
)
from crowdgit.enums import RepositoryState
from crowdgit.errors import (
    InternalError,
    ParentRepoInvalidError,
    ReOnboardingRequiredError,
    RepoAuthRequiredError,
)

# Import configured loguru logger from crowdgit.logger
from crowdgit.logger import logger
from crowdgit.models import Repository
from crowdgit.services import (
    AffiliationService,
    CloneService,
    CommitService,
    LicenseService,
    MaintainerService,
    QueueService,
    SoftwareValueService,
    VulnerabilityScannerService,
)
from crowdgit.services.utils import get_repo_name
from crowdgit.settings import (
    WORKER_ERROR_BACKOFF_SEC,
    WORKER_POLLING_INTERVAL_SEC,
)


class RepositoryWorker:
    """Worker that processes repositories through services"""

    def __init__(
        self,
        clone_service: CloneService,
        commit_service: CommitService,
        software_value_service: SoftwareValueService,
        vulnerability_scanner_service: VulnerabilityScannerService,
        maintainer_service: MaintainerService,
        affiliation_service: AffiliationService,
        license_service: LicenseService,
        queue_service: QueueService,
    ):
        self.clone_service = clone_service
        self.commit_service = commit_service
        self.software_value_service = software_value_service
        self.vulnerability_scanner_service = vulnerability_scanner_service
        self.maintainer_service = maintainer_service
        self.affiliation_service = affiliation_service
        self.license_service = license_service
        self.queue_service = queue_service
        self._shutdown = False

    async def run(self):
        """Run the worker processing loop"""
        logger.info("Starting repository worker")

        try:
            await self._run()
            logger.info("Worker _run() method completed")
        except Exception as e:
            logger.error("Worker failed: {}", e)
            raise InternalError("Repository worker failed") from e
        finally:
            logger.info("Worker run() method exiting")

    async def _run(self):
        """Internal run method with the processing loop"""
        try:
            logger.info("Starting repo worker loop...")
            while not self._shutdown:
                try:
                    await self._process_repositories()
                    await asyncio.sleep(WORKER_POLLING_INTERVAL_SEC)
                except Exception as e:
                    logger.error("Worker error: {}", e)
                    await asyncio.sleep(WORKER_ERROR_BACKOFF_SEC)
            logger.info("Worker loop completed")
        finally:
            await self.queue_service.shutdown()
            logger.info("Worker processing loop completed")

    async def shutdown(self):
        """Shutdown the worker and all its services"""
        logger.info("Shutting down repository worker")
        self._shutdown = True

        logger.info("Worker services shutdown triggered")

    async def _process_repositories(self):
        """
        Process repositories by priority - check acquire_repo_for_processing()
        For this inital version, we'll process repos one by one
        TODO: introduce concurrent processing of multiple repos if needed
        """
        available_repo_to_process = None
        try:
            available_repo_to_process = await acquire_repo_for_processing()

            if not available_repo_to_process:
                logger.debug("No repositories to process")
                return

            await self._process_single_repository(available_repo_to_process)
        except Exception as e:
            logger.error(
                f"Failed to process repository {available_repo_to_process} with error {e}"
            )
        finally:
            if available_repo_to_process:
                logger.info(f"releasing repo: {available_repo_to_process.url}")
                await release_repo(available_repo_to_process.id)
                logger.info(f"Repo {available_repo_to_process.url} released!")

    def _bind_repository_context(self, repository: Repository, repo_name: str) -> None:
        """Bind repository context to all services for enhanced logging

        Args:
            repository: Repository object containing metadata
            repo_name: Parsed repository name for logging
        """
        # Define services with their operation names
        services_operations = [
            (self.clone_service, "cloning"),
            (self.commit_service, "commit_processing"),
            (self.maintainer_service, "maintainer_processing"),
            (self.affiliation_service, "affiliation_processing"),
            (self.software_value_service, "software_value_processing"),
            (self.vulnerability_scanner_service, "vulnerability_scan_processing"),
            (self.license_service, "license_detection"),
            (self.queue_service, "queue_service"),
        ]

        # Bind consistent context for all services: repo_name, id, and operation
        for service, operation in services_operations:
            service.bind_context(repo=repo_name, id=repository.id, operation=operation)

    def _reset_all_contexts(self) -> None:
        """Reset logger context for all services"""
        services = [
            self.clone_service,
            self.commit_service,
            self.maintainer_service,
            self.affiliation_service,
            self.software_value_service,
            self.vulnerability_scanner_service,
            self.license_service,
            self.queue_service,
        ]

        for service in services:
            service.reset_logger_context()

    async def _validate_and_get_parent_repo(self, repository: Repository) -> Repository | None:
        """
        Validate and return the parent repository for forks.
        For forked repos, we need to prevent re-processing activities from the parent repo,
        so we verify the parent:
            1. Is already connected/onboarded in our system
            2. Was processed successfully within REPOSITORY_UPDATE_INTERVAL_HOURS
            3. Has COMPLETED state

        Returns:
            Repository | None: Parent repository if this is a valid fork, None if not a fork
        Raises:
            ParentRepoInvalidError: If this is a fork but the parent repo is invalid or not found
        """
        if not repository.forked_from:
            return None

        logger.info(
            f"Repository {repository.url} is forked from {repository.forked_from}, validating parent repo..."
        )

        parent_repo = await get_recently_processed_repository_by_url(repository.forked_from)
        if not parent_repo:
            logger.warning(
                f"Parent repo {repository.forked_from} is not found/valid - Aborting processing"
            )
            raise ParentRepoInvalidError()

        logger.info(
            f"Parent repo {repository.forked_from} is valid, proceeding with fork processing"
        )
        return parent_repo

    async def _process_single_repository(self, repository: Repository):
        """Process a single repository through services with full clone for new repos, incremental for existing"""
        logger.info("Processing repository: {}", repository.url)
        processing_state = RepositoryState.PENDING

        try:
            repo_name = get_repo_name(repository.url)
            self._bind_repository_context(repository, repo_name)

            # Validate and get parent repo if this is a fork
            repository.parent_repo = await self._validate_and_get_parent_repo(repository)

            async for batch_info in self.clone_service.clone_batches_generator(
                repository,
                working_dir_cleanup=True,
            ):
                logger.info(f"Clone batch info: {batch_info}")
                if batch_info.is_first_batch:
                    await self.software_value_service.run(repository.id, batch_info.repo_path)
                    await self.vulnerability_scanner_service.run(
                        repository.id, batch_info.repo_path, repository.url
                    )
                    await self.maintainer_service.process_maintainers(repository, batch_info)
                    await self.affiliation_service.process_affiliations(repository, batch_info)
                    licenses = await self.license_service.detect(batch_info.repo_path)
                    await update_repository_licenses(repository.id, licenses)
                if batch_info.is_final_batch:
                    await self.commit_service.process_batch_commits(
                        repository,
                        batch_info,
                    )

            logger.info("Incremental processing completed successfully")
            processing_state = RepositoryState.COMPLETED
        except ReOnboardingRequiredError:
            logger.info(f"Repo {repository.url} needs re-onboarding, deferring until weekend")
            processing_state = RepositoryState.PENDING_REONBOARD
        except ParentRepoInvalidError as e:
            logger.error(f"Parent repo validation failed: {repr(e)}")
            processing_state = RepositoryState.REQUIRES_PARENT
        except RepoAuthRequiredError as e:
            logger.error(f"Repository requires authentication: {repr(e)}")
            processing_state = RepositoryState.AUTH_REQUIRED
        except Exception as e:
            logger.error(f"Processing failed with error: {repr(e)}")
            processing_state = RepositoryState.FAILED
        finally:
            # Reset logger context for all services
            self._reset_all_contexts()

            logger.info(f"Updating ${repository.url} state to ${processing_state}")
            await mark_repo_as_processed(repository.id, processing_state)

        logger.info("Completed processing repository: {}", repository.url)
