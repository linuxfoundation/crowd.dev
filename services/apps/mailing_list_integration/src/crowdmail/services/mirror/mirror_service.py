"""Mirror service: keeps a local public-inbox git mirror of a lore mailing
list in sync, and exposes the per-shard git plumbing the worker needs to
find new messages.
"""

import asyncio
import os

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from crowdmail.errors import (
    CommandTimeoutError,
    MirrorError,
    NetworkError,
    RateLimitError,
    RemoteServerError,
    ValidationError,
)
from crowdmail.logger import logger
from crowdmail.services.parse.noteren import get_email_from_git, git_run_command
from crowdmail.settings import LORE_MIRROR_DIR

LORE_BASE_URL = "https://lore.kernel.org/"

RETRYABLE_MIRROR_ERRORS = (NetworkError, RateLimitError, RemoteServerError)

retry_on_mirror_error = retry(
    retry=retry_if_exception_type(RETRYABLE_MIRROR_ERRORS),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5, max=120),
    reraise=True,
)

# (stderr substrings, exception to raise) — checked in order, first match wins
ERROR_CLASSIFICATIONS = [
    (
        {
            "Connection refused",
            "Network is unreachable",
            "Connection timed out",
            "Could not resolve host",
        },
        NetworkError,
    ),
    ({"429", "Too Many Requests"}, RateLimitError),
    ({"500", "502", "503", "504"}, RemoteServerError),
]


def _classify_and_raise(stderr_text: str, command_str: str) -> None:
    for patterns, error_class in ERROR_CLASSIFICATIONS:
        if any(pattern in stderr_text for pattern in patterns):
            logger.error(f"{error_class.__name__}: {stderr_text}")
            raise error_class(f"{error_class.__name__} while running: {command_str}")
    logger.error(f"Mirror command failed: {stderr_text}")
    raise MirrorError(f"Command failed: {command_str} - {stderr_text}")


async def _run_shell_command(cmd: list[str], cwd: str | None = None, timeout: float = 300) -> str:
    command_str = " ".join(cmd)
    process = await asyncio.create_subprocess_exec(
        *cmd, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise CommandTimeoutError(f"Command timed out after {timeout}s: {command_str}") from None

    stdout_text = stdout.decode(errors="replace").strip()
    stderr_text = stderr.decode(errors="replace").strip()
    if process.returncode == 0:
        return stdout_text
    _classify_and_raise(stderr_text, command_str)
    return ""


MAX_LIST_NAME_LENGTH = 255


def _validate_list_name(list_name: str) -> None:
    """Reject anything that could escape the mirror root as a path component.

    `list_name` becomes a single os.path.join component (no separators
    allowed), so the only way it can resolve outside `mirror_dir` is by being
    exactly "." or "..". Everything else is a literal directory name.
    """
    if (
        not list_name
        or len(list_name) > MAX_LIST_NAME_LENGTH
        or "/" in list_name
        or "\0" in list_name
        or list_name in (".", "..")
    ):
        raise ValidationError(f"Invalid mailing list name: {list_name!r}")


def list_mirror_dir(list_name: str, mirror_dir: str = LORE_MIRROR_DIR) -> str:
    """Local directory where a lore list mirror lives."""
    _validate_list_name(list_name)
    return os.path.join(mirror_dir, list_name)


@retry_on_mirror_error
async def ensure_mirror(list_name: str, mirror_dir: str = LORE_MIRROR_DIR) -> str:
    """Clone the list mirror if it's absent, otherwise fetch new commits. Returns the list dir."""
    list_dir = list_mirror_dir(list_name, mirror_dir)
    if not os.path.isdir(list_dir):
        logger.info(f"Cloning lore list '{list_name}' into {list_dir}")
        os.makedirs(os.path.dirname(list_dir), exist_ok=True)
        await _run_shell_command(
            ["public-inbox-clone", "-q", f"{LORE_BASE_URL}{list_name}/", list_dir]
        )
        # public-inbox-clone drops a manifest scoped to clone time; remove it
        # and re-fetch from inside list_dir so the manifest is regenerated
        # correctly there (needed to detect new epoch shards, e.g. git/1.git,
        # as the list grows).
        manifest_path = os.path.join(list_dir, "manifest.js.gz")
        if os.path.exists(manifest_path):
            os.remove(manifest_path)
        await _run_shell_command(["public-inbox-fetch", "-q"], cwd=list_dir)
    else:
        logger.info(f"Fetching updates for lore list '{list_name}'")
        await _run_shell_command(["public-inbox-fetch", "-q", "-C", list_dir])
    return list_dir


def discover_shards(list_dir: str) -> list[str]:
    """Return absolute paths of numbered shard dirs (e.g. 0.git, 1.git, ...) under list_dir/git/."""
    git_dir = os.path.join(list_dir, "git")
    if not os.path.isdir(git_dir):
        return []
    return sorted(
        os.path.join(git_dir, entry) for entry in os.listdir(git_dir) if entry.endswith(".git")
    )


def shard_index(shard_path: str) -> str:
    """Numeric shard id ('0', '1', ...) from a shard path like .../git/3.git."""
    return os.path.basename(shard_path).removesuffix(".git")


async def new_commits(shard_path: str, since_sha: str | None) -> list[str]:
    """Commit ids in a shard newer than since_sha, oldest first. since_sha=None returns full history."""
    git_range = f"{since_sha}..HEAD" if since_sha else "HEAD"
    ecode, out, err = await asyncio.to_thread(
        git_run_command,
        shard_path,
        ["log", "--reverse", "--format=%H", "--end-of-options", git_range],
    )
    if ecode != 0:
        _classify_and_raise(err.decode(errors="replace").strip(), f"git log {git_range}")
    return [line for line in out.decode().splitlines() if line]


def read_email(shard_path: str, git_id: str) -> tuple[bytes, str]:
    """Raw email bytes + blob id for a commit in a shard."""
    return get_email_from_git(shard_path, git_id)
