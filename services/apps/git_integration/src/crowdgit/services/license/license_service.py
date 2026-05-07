import json

from crowdgit.errors import CommandExecutionError, CommandTimeoutError
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.utils import run_shell_command


class LicenseService(BaseService):
    """Detects SPDX license from a cloned repository using the licensee gem."""

    async def detect(self, repo_path: str) -> str | None:
        """Run licensee against repo_path and return the SPDX identifier, or None."""
        try:
            output = await run_shell_command(["licensee", "detect", "--json", repo_path])
        except CommandExecutionError:
            self.logger.info(f"licensee found no license in {repo_path}")
            return None
        except CommandTimeoutError as e:
            self.logger.warning(f"licensee timed out: {repr(e)}")
            return None
        except FileNotFoundError as e:
            self.logger.warning(f"licensee binary not found in PATH: {repr(e)}")
            return None
        except Exception as e:
            self.logger.warning(f"licensee failed: {repr(e)}")
            return None

        try:
            data = json.loads(output)
            matched = data.get("matched_license") or {}
            spdx_id = matched.get("spdx_id")
            confidence = matched.get("confidence")
            if spdx_id:
                self.logger.info(f"License detected: {spdx_id} (confidence={confidence}) in {repo_path}")
            else:
                self.logger.info(f"No SPDX license matched in {repo_path}")
            return spdx_id
        except Exception as e:
            self.logger.warning(f"Failed to parse licensee output: {repr(e)}")
            return None
