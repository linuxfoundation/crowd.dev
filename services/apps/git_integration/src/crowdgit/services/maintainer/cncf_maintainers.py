from pathlib import Path

import yaml

from crowdgit.models.maintainer_info import MaintainerInfoItem
from crowdgit.services.utils import parse_repo_url

CNCF_MAINTAINERS_FILENAMES = ("maintainers.yaml", "maintainers.yml")


def is_cncf_repo(repo_url: str) -> bool:
    try:
        _, repo_name = parse_repo_url(repo_url)
    except Exception:
        return False
    return repo_name.lower() == ".project"


def find_cncf_maintainers_file(repo_path: Path) -> Path | None:
    repo_path = Path(repo_path)
    for filename in CNCF_MAINTAINERS_FILENAMES:
        candidate = repo_path / filename
        if candidate.is_file():
            return candidate
    return None


def parse_cncf_maintainers_yaml(content: str) -> list[MaintainerInfoItem] | None:
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError:
        return None

    if not isinstance(data, dict):
        return None

    maintainer_entries = data.get("maintainers")
    if not isinstance(maintainer_entries, list):
        return None

    members: list[str] = []
    for entry in maintainer_entries:
        if not isinstance(entry, dict):
            return None
        teams = entry.get("teams")
        if not isinstance(teams, list):
            return None
        for team in teams:
            if not isinstance(team, dict):
                return None
            team_members = team.get("members")
            if not isinstance(team_members, list):
                return None
            members.extend(team_members)

    if not members:
        return None

    return [
        MaintainerInfoItem(github_username=member, normalized_title="maintainer")
        for member in members
    ]
