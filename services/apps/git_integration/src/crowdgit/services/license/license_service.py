import json

from crowdgit.errors import CommandExecutionError, CommandTimeoutError
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.utils import run_shell_command

# Mirrors GitHub's internal licensee threshold — matches below this are too uncertain.
LICENSE_CONFIDENCE_THRESHOLD = 98


class LicenseService(BaseService):
    """Detects SPDX license from a cloned repository using the licensee gem."""

    async def detect(self, repo_path: str) -> str | None:
        """Run licensee against repo_path and return the SPDX identifier, or None."""
        try:
            output = await run_shell_command(
                ["licensee", "detect", "--json", repo_path], timeout=60
            )
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
            licenses = data.get("licenses") or []
            matched_files = data.get("matched_files") or []
            spdx_id = licenses[0].get("spdx_id") if licenses else None
            confidence = (
                (matched_files[0].get("matcher") or {}).get("confidence")
                if matched_files
                else None
            )

            # Mirror GitHub's threshold — below LICENSE_CONFIDENCE_THRESHOLD the match is unreliable.
            # Downgrade low-confidence matches to NOASSERTION so the distinction is clean:
            #   NULL         = licensee didn't run, timed out, or found no license file
            #   NOASSERTION  = found a license file but couldn't reliably identify it
            # The UI should display NOASSERTION as "Other".
            if spdx_id and spdx_id != "NOASSERTION" and confidence is not None and confidence < LICENSE_CONFIDENCE_THRESHOLD:
                self.logger.info(
                    f"License downgraded to NOASSERTION: confidence {confidence}% below threshold in {repo_path}"
                )
                return "NOASSERTION"

            if spdx_id:
                self.logger.info(
                    f"License detected: {spdx_id} (confidence={confidence}) in {repo_path}"
                )
            else:
                self.logger.info(f"No SPDX license matched in {repo_path}")
            return spdx_id
        except Exception as e:
            self.logger.warning(f"Failed to parse licensee output: {repr(e)}")
            return None
