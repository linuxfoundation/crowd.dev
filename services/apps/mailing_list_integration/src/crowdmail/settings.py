import os


def load_env_var(key: str, required=True, default=None):
    value = os.getenv(key) or default
    if required and value is None:
        raise OSError(f"Missing required environment variable: {key}")
    return value


CROWD_DB_WRITE_HOST = load_env_var("CROWD_DB_WRITE_HOST")
CROWD_DB_PORT = int(load_env_var("CROWD_DB_PORT"))
CROWD_DB_USERNAME = load_env_var("CROWD_DB_USERNAME")
CROWD_DB_PASSWORD = load_env_var("CROWD_DB_PASSWORD")
CROWD_DB_DATABASE = load_env_var("CROWD_DB_DATABASE")

WORKER_POLLING_INTERVAL_SEC = int(load_env_var("WORKER_POLLING_INTERVAL_SEC", default=5))
WORKER_ERROR_BACKOFF_SEC = int(load_env_var("WORKER_ERROR_BACKOFF_SEC", default=10))
WORKER_SHUTDOWN_TIMEOUT_SEC = int(load_env_var("WORKER_SHUTDOWN_TIMEOUT_SEC", default="3600"))

DEFAULT_TENANT_ID = load_env_var(
    "CROWD_SSO_LF_TENANT_ID", default="875c38bd-2b1b-4e91-ad07-0cfbabb4c49f"
)

CROWD_KAFKA_BROKERS = load_env_var("CROWD_KAFKA_BROKERS")
CROWD_KAFKA_TOPIC = load_env_var("CROWD_KAFKA_TOPIC")
CROWD_KAFKA_EXTRA = load_env_var("CROWD_KAFKA_EXTRA", required=False)

# Directory where public-inbox git mirrors are kept, one subdir per list
LORE_MIRROR_DIR = load_env_var("LORE_MIRROR_DIR", default="/var/lore")

MAX_CONCURRENT_ONBOARDINGS = int(load_env_var("MAX_CONCURRENT_ONBOARDINGS", default="3"))
LIST_UPDATE_INTERVAL_HOURS = int(load_env_var("LIST_UPDATE_INTERVAL_HOURS", default=24))
FAILED_RETRY_INTERVAL_HOURS = int(load_env_var("FAILED_RETRY_INTERVAL_HOURS", default="6"))

# A list stuck "locked" (e.g. worker pod died mid-processing) past this many hours is
# treated as reclaimable, so it doesn't stay blocked forever.
STUCK_ONBOARDING_LIST_TIMEOUT_HOURS = int(
    load_env_var("STUCK_ONBOARDING_LIST_TIMEOUT_HOURS", default="12")
)
STUCK_RECURRENT_LIST_TIMEOUT_HOURS = int(
    load_env_var("STUCK_RECURRENT_LIST_TIMEOUT_HOURS", default="4")
)

# Flush accumulated activities (and checkpoint processed heads) every this many
# messages instead of buffering an entire list's history in memory before one
# flush at the end — large lore archives can have 100k+ messages.
ACTIVITY_FLUSH_BATCH_SIZE = int(load_env_var("ACTIVITY_FLUSH_BATCH_SIZE", default="500"))

# Initial public-inbox-clone of a large archive (e.g. linux-kernel) can legitimately
# take much longer than the default command timeout; incremental fetches stay short.
CLONE_TIMEOUT_SEC = int(load_env_var("CLONE_TIMEOUT_SEC", default="3600"))
