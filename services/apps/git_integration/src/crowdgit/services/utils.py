import asyncio
import logging
import re
from urllib.parse import urlparse

from crowdgit.errors import (
    CommandExecutionError,
    CommandTimeoutError,
    DiskSpaceError,
    NetworkError,
    PermissionError,
    ValidationError,
)
from crowdgit.logger import logger


def _safe_decode(data: bytes) -> str:
    """
    Safely decode bytes to string, handling various encodings that might be present in git output.

    Git repositories can contain commit messages and other text in various encodings.
    This function attempts to decode using UTF-8 first, then tries more specific encodings
    (CP1252, ISO-8859-1) before falling back to UTF-8 with error replacement, which
    replaces invalid bytes with the Unicode replacement character (�).

    Args:
        data: Raw bytes to decode

    Returns:
        str: Decoded string using the most appropriate encoding, with invalid bytes replaced if necessary
    """
    if not data:
        return ""

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning("Failed to decode output to utf-8")
        pass

    # CP1252 is common for Windows-generated content and has specific byte mappings
    # ISO-8859-1 is a common legacy encoding for Western European languages
    for encoding in ("iso-8859-1", "cp1252"):
        logger.info(f"Trying {encoding} decoding")
        try:
            return data.decode(encoding)
        except Exception:
            continue
    logger.warning("Fallback to utf-8 decoding with replacement")
    # Final fallback: UTF-8 with error replacement (replaces invalid bytes with �)
    return data.decode("utf-8", errors="replace")


def parse_repo_url(repo_url: str):
    """
    Parse repository url and returns owner and repo_name
    """
    parsed_url = urlparse(repo_url)
    path_parts = parsed_url.path.strip("/").split("/")

    if len(path_parts) >= 2:
        return path_parts[:2]
    raise ValidationError("Failed to get owner and repo_name from repo_url")


def get_repo_name(remote: str) -> str:
    """
    Get the domain and path segments from the remote URL and join them with '-'.

    Args:
        remote: The remote URL of the repository.

    Returns:
        The domain and path segments joined by '-', without the '.git' extension.

    Examples:
        >>> get_repo_name("https://github.com/user/repo.git")
        'github.com-user-repo'

        >>> get_repo_name("https://gerrit.fd.io/r/hc2vpp")
        'gerrit.fd.io-r-hc2vpp'
    """
    # Remove the protocol (http or https)
    remote = re.sub(r"^https?://", "", remote)
    # Remove the '.git' extension if present
    if remote.endswith(".git"):
        remote = remote[:-4]
    # Split by '/' and join with '-'
    parts = remote.strip().rstrip("/").split("/")
    return "-".join(parts)


async def get_remote_default_branch(remote_url: str) -> str | None:
    """Get the default branch of a remote repository without cloning

    Args:
        remote_url: The URL of the remote repository.

    Returns:
        The default branch name, or None if unable to determine.
    """
    try:
        # Use git ls-remote to get the symbolic reference for HEAD
        output = await run_shell_command(["git", "ls-remote", "--symref", remote_url, "HEAD"])

        # Parse the output to find the symbolic reference
        # Output format: "ref: refs/heads/main\tHEAD\n<commit_hash>\tHEAD"
        lines = output.strip().split("\n")
        for line in lines:
            if line.startswith("ref: refs/heads/"):
                # Extract branch name from "ref: refs/heads/main"
                return line.split("refs/heads/")[-1].split("\t")[0]

        # Fallback: if symbolic ref not available, try common branches
        for branch in ["main", "master"]:
            try:
                await run_shell_command(["git", "ls-remote", "--heads", remote_url, branch])
                return branch
            except CommandExecutionError:
                continue

        return None

    except CommandExecutionError as e:
        logger.warning(f"Failed to get remote default branch for {remote_url}: {e}")
        return None


async def get_default_branch(repo_path: str) -> str:
    """Get the default branch of the repository using local repo

    Args:
        repo_path: The local path to the repository.

    Returns:
        The default branch name.
    """

    try:
        output = await run_shell_command(
            ["git", "-C", repo_path, "symbolic-ref", "refs/remotes/origin/HEAD"]
        )

        # Remove the 'refs/remotes/origin/' prefix to get the full branch name
        prefix = "refs/remotes/origin/"
        if output.startswith(prefix):
            return output[len(prefix) :]
    except CommandExecutionError:
        pass

    # Fallback: check which common branches exist locally as remote tracking branches
    for branch in ["master", "main"]:
        try:
            await run_shell_command(
                ["git", "-C", repo_path, "show-ref", "--verify", f"refs/remotes/origin/{branch}"]
            )
            return branch
        except CommandExecutionError:
            continue

    # Final fallback: detached mode
    logger.warning(f"No remote tracking branches found for {repo_path}, assuming detached mode")
    return "*"


async def run_shell_command(
    cmd: list[str],
    cwd: str = None,
    timeout: float | None = None,
    input_text: str | bytes | None = None,
    stderr_logger: logging.Logger | None = None,
) -> str:
    """
    Run shell command asynchronously and return output on success, raise exception on failure.

    Args:
        cmd: Command and arguments
        cwd: Working directory
        timeout: Command timeout in seconds
        input_text: Text (str) or bytes to send to stdin (will automatically append newline if not present)
        stderr_logger: If provided, a logger whose .info() method is called with each stderr line in real-time

    Returns:
        str: Command stdout output

    Raises:
        CommandTimeoutError: When command times out
        DiskSpaceError: When disk space is insufficient
        NetworkError: When network connectivity issues occur
        PermissionError: When permission is denied
        CommandExecutionError: For other command failures
    """
    process = None
    command_str = " ".join(cmd)
    try:
        # Create subprocess with stdin pipe if input is provided
        if input_text is not None:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE,
            )
        else:
            process = await asyncio.create_subprocess_exec(
                *cmd, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

        # Prepare input if provided
        stdin_input = None
        if input_text is not None:
            # Handle both string and bytes input
            if isinstance(input_text, bytes):
                # If input is already bytes, ensure it ends with newline
                if not input_text.endswith(b"\n"):
                    input_text += b"\n"
                stdin_input = input_text
            else:
                # If input is string, ensure it ends with newline then encode
                if not input_text.endswith("\n"):
                    input_text += "\n"
                stdin_input = input_text.encode("utf-8")

        if stderr_logger:
            stderr_lines: list[str] = []

            async def _run_with_stderr_logging() -> bytes:
                async def _stream() -> None:
                    async for raw_line in process.stderr:
                        line = _safe_decode(raw_line).rstrip()
                        if line:
                            stderr_logger.info(line)
                            stderr_lines.append(line)

                stdout, _ = await asyncio.gather(process.stdout.read(), _stream())
                await process.wait()
                return stdout

            coro = _run_with_stderr_logging()
            stdout = await (asyncio.wait_for(coro, timeout=timeout) if timeout else coro)
            stdout_text = _safe_decode(stdout).strip() if stdout else ""
            stderr_text = "\n".join(stderr_lines)
        else:
            # Wait for completion with optional timeout
            if timeout:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(input=stdin_input), timeout=timeout
                )
            else:
                stdout, stderr = await process.communicate(input=stdin_input)

            # Handle potentially non-UTF-8 encoded output from git commands
            stdout_text = _safe_decode(stdout).strip() if stdout else ""
            stderr_text = _safe_decode(stderr).strip() if stderr else ""

        # Check return code
        if process.returncode == 0:
            return stdout_text

        if "No space left on device" in stderr_text:
            logger.error(f"Disk space error: {stderr_text}")
            raise DiskSpaceError(f"Disk space error while running: {command_str}")
        elif any(
            pattern in stderr_text
            for pattern in ["Network is unreachable", "Connection refused", "Connection timed out"]
        ):
            logger.warning(f"Network error: {stderr_text}")
            raise NetworkError(f"Network error while running: {command_str}")
        elif "Permission denied" in stderr_text:
            logger.error(f"Permission error: {stderr_text}")
            raise PermissionError(f"Permission denied while running: {command_str}")
        else:
            logger.error(f"Command failed (exit {process.returncode}): {stderr_text}")
            raise CommandExecutionError(
                f"Command failed (exit {process.returncode}): {command_str} - {stderr_text}",
                returncode=process.returncode,
            )

    except asyncio.TimeoutError:
        logger.error(f"Command timed out after {timeout}s: {command_str}")
        # Kill the process if it's still running
        if process and process.returncode is None:
            process.kill()
            await process.wait()
        raise CommandTimeoutError(f"Command timed out after {timeout}s: {command_str}") from None
