from enum import Enum


class ErrorCode(str, Enum):
    """Standard Error codes"""

    UNKNOWN = "unknown"
    INTERNAL = "server-error"
    SHELL_COMMAND_TIMEOUT = "shell-command-timeout"
    DISK_SPACE = "disk-space-error"
    NETWORK_ERROR = "network-error"
    PERMISSION_ERROR = "permission-error"
    SHELL_COMMAND_FAILED = "shell-command-failed"
    QUEUE_EMIT_ERROR = "queue-emit-error"
    QUEUE_CONNECTION_ERROR = "queue-connection-error"
    VALIDATION = "validation"
    RATE_LIMITED = "rate-limited"
    SERVER_ERROR = "server-error-remote"


class ListState(str, Enum):
    """Mailing list processing states"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING_REONBOARD = "pending_reonboard"  # re-onboarding deferred until weekend


class ListPriority(int):
    """Mailing list processing priorities"""

    URGENT = 0
    HIGH = 1
    NORMAL = 2


class IntegrationResultType(str, Enum):
    ACTIVITY = "activity"


class IntegrationResultState(str, Enum):
    PENDING = "pending"


class DataSinkWorkerQueueMessageType(str, Enum):
    PROCESS_INTEGRATION_RESULT = "process_integration_result"


class ActivityType(str, Enum):
    """Groupsio-compatible activity types (platform=groupsio)"""

    MESSAGE = "message"
    MEMBER_JOIN = "member_join"
    MEMBER_LEAVE = "member_leave"


class ExecutionStatus(str, Enum):
    """Service execution status"""

    SUCCESS = "success"
    FAILURE = "failure"


class OperationType(str, Enum):
    """Service operation types for metrics tracking"""

    MIRROR = "Mirror"
    PARSE = "Parse"
