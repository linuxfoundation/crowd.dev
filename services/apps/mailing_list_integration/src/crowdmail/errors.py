from dataclasses import dataclass

from crowdmail.enums import ErrorCode


@dataclass
class CrowdMailError(Exception):
    error_message: str = "An unknown error occurred"
    error_code: ErrorCode | None = ErrorCode.UNKNOWN


@dataclass
class InternalError(CrowdMailError):
    error_message: str = "Internal error"
    error_code: ErrorCode = ErrorCode.INTERNAL


@dataclass
class ListLockingError(CrowdMailError):
    error_message: str = "Cannot acquire list lock to start processing"
    error_code: ErrorCode = ErrorCode.INTERNAL


@dataclass
class CommandTimeoutError(CrowdMailError):
    error_message: str = "Command execution timed out"
    error_code: ErrorCode = ErrorCode.SHELL_COMMAND_TIMEOUT


@dataclass
class CommandExecutionError(CrowdMailError):
    error_message: str = "Command execution failed"
    error_code: ErrorCode = ErrorCode.SHELL_COMMAND_FAILED
    returncode: int | None = None


@dataclass
class NetworkError(CrowdMailError):
    error_message: str = "Network connection error"
    error_code: ErrorCode = ErrorCode.NETWORK_ERROR


@dataclass
class RateLimitError(CrowdMailError):
    error_message: str = "Rate limited by remote server"
    error_code: ErrorCode = ErrorCode.RATE_LIMITED


@dataclass
class RemoteServerError(CrowdMailError):
    error_message: str = "Remote server returned an internal error"
    error_code: ErrorCode = ErrorCode.SERVER_ERROR


@dataclass
class MirrorError(CrowdMailError):
    error_message: str = "Failed to mirror mailing list"
    error_code: ErrorCode = ErrorCode.INTERNAL


@dataclass
class QueueConnectionError(CrowdMailError):
    error_message: str = "Failed to connect to queue"
    error_code: ErrorCode = ErrorCode.QUEUE_CONNECTION_ERROR


@dataclass
class QueueMessageProduceError(CrowdMailError):
    error_message: str = "Failed to emit message to queue"
    error_code: ErrorCode = ErrorCode.QUEUE_EMIT_ERROR


@dataclass
class ValidationError(CrowdMailError):
    error_message: str = "Failed to parse/validate list field(s)"
    error_code: ErrorCode = ErrorCode.VALIDATION
