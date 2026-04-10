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
    NO_MAINTAINER_FILE = "no-maintainer-file"
    NO_MAINTAINER_FOUND = "no-maintainer-found"
    MAINTAINER_ANALYSIS_FAILED = "maintainer-analysis-failed"
    MAINTAINER_INTERVAL_NOT_ELAPSED = "maintainer-interval-not-elapsed"
    CLEANUP_FAILED = "cleanup-failed"
    PARENT_REPO_INVALID = "parent-repo-invalid"
    REONBOARDING_REQUIRED = "reonboarding-required"
    STUCK_REPO = "stuck-repo"
    REPO_AUTH_REQUIRED = "repo-auth-required"
    RATE_LIMITED = "rate-limited"
    ACCESS_FORBIDDEN = "access-forbidden"
    SERVER_ERROR = "server-error-remote"


class RepositoryState(str, Enum):
    """Repository processing states"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    REQUIRES_PARENT = "requires_parent"  # fork repo without valid parent repo in out system
    STUCK = "stuck"  # requires manual resolution
    PENDING_REONBOARD = "pending_reonboard"  # re-onboarding deferred until weekend
    AUTH_REQUIRED = "auth_required"  # private repo or repo requiring authentication


class RepositoryPriority(int):
    """Repository processing priorities"""

    URGENT = 0
    HIGH = 1
    NORMAL = 2


class IntegrationResultType(str, Enum):
    ACTIVITY = "activity"


class IntegrationResultState(str, Enum):
    PENDING = "pending"


class DataSinkWorkerQueueMessageType(str, Enum):
    PROCESS_INTEGRATION_RESULT = "process_integration_result"


class ExecutionStatus(str, Enum):
    """Service execution status"""

    SUCCESS = "success"
    FAILURE = "failure"


class OperationType(str, Enum):
    """Service operation types for metrics tracking"""

    CLONE = "Clone"
    COMMIT = "Commit"
    MAINTAINER = "Maintainer"
    SOFTWARE_VALUE = "SoftwareValue"
    VULNERABILITY_SCAN = "VulnerabilityScanner"
