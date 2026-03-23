from dataclasses import dataclass

from crowdgit.enums import ErrorCode


@dataclass
class CrowdGitError(Exception):
    error_message: str = "An unknown error occurred"
    error_code: ErrorCode | None = ErrorCode.UNKNOWN
    ai_cost: int | None = 0


@dataclass
class InternalError(CrowdGitError):
    error_message: str = "Internal error"
    error_code: ErrorCode = ErrorCode.INTERNAL


@dataclass
class UnkownError(CrowdGitError):
    error_message: str = "Unkown error"
    error_code: ErrorCode = ErrorCode.UNKNOWN


@dataclass
class RepoLockingError(CrowdGitError):
    error_message: str = "Cannot acquire repo lock to start processing"
    error_code: ErrorCode = ErrorCode.INTERNAL


@dataclass
class CommandTimeoutError(CrowdGitError):
    error_message: str = "Command execution timed out"
    error_code: ErrorCode = ErrorCode.SHELL_COMMAND_TIMEOUT


@dataclass
class DiskSpaceError(CrowdGitError):
    error_message: str = "Insufficient disk space"
    error_code: ErrorCode = ErrorCode.DISK_SPACE


@dataclass
class NetworkError(CrowdGitError):
    error_message: str = "Network connection error"
    error_code: ErrorCode = ErrorCode.NETWORK_ERROR


@dataclass
class PermissionError(CrowdGitError):
    error_message: str = "Permission denied"
    error_code: ErrorCode = ErrorCode.PERMISSION_ERROR


@dataclass
class CommandExecutionError(CrowdGitError):
    error_message: str = "Command execution failed"
    error_code: ErrorCode = ErrorCode.SHELL_COMMAND_FAILED
    returncode: int | None = None


@dataclass
class CloneError(CrowdGitError):
    error_message: str = "Failed to clone repository"
    error_code: ErrorCode = ErrorCode.INTERNAL


@dataclass
class QueueConnectionError(CrowdGitError):
    error_message: str = "Failed to connect to queue"
    error_code: ErrorCode = ErrorCode.QUEUE_CONNECTION_ERROR


@dataclass
class QueueMessageProduceError(CrowdGitError):
    error_message: str = "Failed to emit message to queue"
    error_code: ErrorCode = ErrorCode.QUEUE_EMIT_ERROR


@dataclass
class ValidationError(CrowdGitError):
    error_message: str = "Failed to parse/validate repo field(s)"
    error_code: ErrorCode = ErrorCode.VALIDATION


@dataclass
class MaintainerFileNotFoundError(CrowdGitError):
    error_message: str = "No maintainer file found"
    error_code: ErrorCode = ErrorCode.NO_MAINTAINER_FILE
    ai_cost: int = 0


@dataclass
class MaintanerAnalysisError(CrowdGitError):
    error_message: str = "Failed to analyze maintainer file content"
    error_code: ErrorCode = ErrorCode.MAINTAINER_ANALYSIS_FAILED
    ai_cost: int = 0


@dataclass
class MaintainerIntervalNotElapsedError(CrowdGitError):
    error_message: str = "Maintainer processing interval has not elapsed yet"
    error_code: ErrorCode = ErrorCode.MAINTAINER_INTERVAL_NOT_ELAPSED
    ai_cost: int = 0


@dataclass
class ParentRepoInvalidError(CrowdGitError):
    error_message: str = "Parent repository is not valid or not found"
    error_code: ErrorCode = ErrorCode.PARENT_REPO_INVALID


@dataclass
class ReOnboardingRequiredError(CrowdGitError):
    error_message = "Repository cannot be processed and requires re-onboarding"
    error_code: ErrorCode = ErrorCode.REONBOARDING_REQUIRED


@dataclass
class StuckRepoError(CrowdGitError):
    error_message = "Repos stuck in processing state for a long time"
    error_code: ErrorCode = ErrorCode.STUCK_REPO
