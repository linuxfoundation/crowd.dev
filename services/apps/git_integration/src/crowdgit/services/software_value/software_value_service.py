import json
import time
from decimal import Decimal

from crowdgit.database.crud import save_service_execution
from crowdgit.enums import ErrorCode, ExecutionStatus, OperationType
from crowdgit.models.service_execution import ServiceExecution
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.utils import run_shell_command

_LARGE_REPO_THRESHOLD_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
# Repos excluded from software value analysis.
# f7f92577-f258-49f0-b5b4-ba07194ca040: data repo (not a code repo), produces misleading results.
_SOFTWARE_VALUE_EXCLUDED_REPO_IDS = frozenset({"f7f92577-f258-49f0-b5b4-ba07194ca040"})


async def _get_repo_size_bytes(repo_path: str) -> int:
    """Return total disk usage of repo_path in bytes using du -sb."""
    try:
        output = await run_shell_command(["du", "-sb", repo_path], timeout=120)
        return int(output.split()[0])
    except Exception:
        pass
    return 0


class SoftwareValueService(BaseService):
    """Service for calculating software value metrics"""

    def __init__(self):
        super().__init__()
        # software-value binary path was defined during Docker build
        self.software_value_executable = "/usr/local/bin/software-value"

    async def run(self, repo_id: str, repo_path: str) -> None:
        """
        Triggers software value binary for given repo.
        Results are saved into insights database directly.
        Repos in _SOFTWARE_VALUE_EXCLUDED_REPO_IDS are skipped entirely.
        For repos larger than 10 GB, scc is run with --no-large (skipping files >100MB) to avoid OOM.
        """
        if repo_id in _SOFTWARE_VALUE_EXCLUDED_REPO_IDS:
            self.logger.info(f"Skipping software value for excluded repo {repo_id}")
            return

        start_time = time.time()
        execution_status = ExecutionStatus.SUCCESS
        error_code = None
        error_message = None

        try:
            cmd = [self.software_value_executable]

            repo_size = await _get_repo_size_bytes(repo_path)
            if repo_size >= _LARGE_REPO_THRESHOLD_BYTES:
                self.logger.info(
                    f"Repo size {repo_size / (1024**3):.1f} GB exceeds threshold — "
                    "running scc with no-large (skipping files >100MB)"
                )
                cmd += ["--no-large"]

            cmd.append(repo_path)

            self.logger.info("Running software value...")
            output = await run_shell_command(cmd)
            self.logger.info(f"Software value output: {output}")

            # Parse JSON output and extract fields from StandardResponse structure
            json_output = json.loads(output)
            status = json_output.get("status")

            if status == "success":
                execution_status = ExecutionStatus.SUCCESS
            else:
                execution_status = ExecutionStatus.FAILURE
                error_code = json_output.get("error_code")
                error_message = json_output.get("error_message")
                self.logger.error(
                    f"Software value processing failed: {error_message} (code: {error_code})"
                )

        except Exception as e:
            execution_status = ExecutionStatus.FAILURE
            error_code = ErrorCode.UNKNOWN.value
            error_message = repr(e)
            self.logger.error(f"Software value processing failed with unexpected error: {repr(e)}")
        finally:
            end_time = time.time()
            execution_time = Decimal(str(round(end_time - start_time, 2)))

            service_execution = ServiceExecution(
                repo_id=repo_id,
                operation_type=OperationType.SOFTWARE_VALUE,
                status=execution_status,
                error_code=error_code,
                error_message=error_message,
                execution_time_sec=execution_time,
            )
            await save_service_execution(service_execution)
