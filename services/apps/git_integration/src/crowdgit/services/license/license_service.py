import json

from crowdgit.errors import CommandExecutionError, CommandTimeoutError
from crowdgit.services.base.base_service import BaseService
from crowdgit.services.utils import run_shell_command

# Mirrors GitHub's internal licensee threshold — matches below this are too uncertain.
LICENSE_CONFIDENCE_THRESHOLD = 98


class LicenseService(BaseService):
    """Detects SPDX license from a cloned repository using the licensee gem."""

    async def detect(self, repo_path: str) -> list[str]:
        """Run licensee against repo_path and return a list of SPDX identifiers.

        Returns [] when licensee is unavailable or finds no license files.
        Returns ['NOASSERTION'] when files are found but none meet the confidence threshold.
        """
        try:
            output = await run_shell_command(
                ["licensee", "detect", "--json", repo_path], timeout=60
            )
        except CommandExecutionError:
            self.logger.info(f"licensee found no license in {repo_path}")
            return []
        except CommandTimeoutError as e:
            self.logger.warning(f"licensee timed out: {repr(e)}")
            return []
        except FileNotFoundError as e:
            self.logger.warning(f"licensee binary not found in PATH: {repr(e)}")
            return []
        except Exception as e:
            self.logger.warning(f"licensee failed: {repr(e)}")
            return []

        try:
            data = json.loads(output)
            licenses = data.get("licenses") or []
            matched_files = data.get("matched_files") or []

            # Build a map from spdx_id to its best confidence across matched files.
            # licensee puts per-file confidence inside each matched_file's matcher object.
            confidence_by_spdx: dict[str, float] = {}
            for mf in matched_files:
                ml = mf.get("matched_license")
                # licensee JSON emits matched_license as a plain string (SPDX id), not a dict
                spdx = ml if isinstance(ml, str) else ((ml or {}).get("spdx_id") or "")
                conf = (mf.get("matcher") or {}).get("confidence")
                if spdx and conf is not None:
                    confidence_by_spdx[spdx] = max(confidence_by_spdx.get(spdx, 0), conf)

            # Mirror GitHub's threshold — below LICENSE_CONFIDENCE_THRESHOLD the match is unreliable.
            # Drop low-confidence entries; if nothing passes, use NOASSERTION:
            #   []              = licensee didn't run, timed out, or found no license file
            #   ['NOASSERTION'] = found a license file but couldn't reliably identify it
            result: list[str] = []
            seen: set[str] = set()
            for entry in licenses:
                spdx_id = entry.get("spdx_id")
                if not spdx_id or spdx_id in seen:
                    continue
                if spdx_id == "NOASSERTION":
                    continue
                confidence = confidence_by_spdx.get(spdx_id)
                if confidence is not None and confidence < LICENSE_CONFIDENCE_THRESHOLD:
                    self.logger.info(
                        f"License {spdx_id} dropped: confidence {confidence}% below threshold in {repo_path}"
                    )
                    continue
                result.append(spdx_id)
                seen.add(spdx_id)

            if not result and licenses:
                self.logger.info(
                    f"All licenses below threshold, storing NOASSERTION in {repo_path}"
                )
                return ["NOASSERTION"]

            if result:
                self.logger.info(f"Licenses detected: {result} in {repo_path}")
            else:
                self.logger.info(f"No SPDX license matched in {repo_path}")
            return result
        except Exception as e:
            self.logger.warning(f"Failed to parse licensee output: {repr(e)}")
            return []
