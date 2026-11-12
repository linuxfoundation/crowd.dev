from pathlib import Path

import yaml

from crowdgit.models.maintainer_info import MaintainerInfoItem

# Project information file defined by LF Release Engineering:
# https://github.com/lfit/releng-global-jjb/blob/master/schema/info-schema.yaml
INFO_YAML_FILENAME = "INFO.yaml"


def find_info_yaml_file(repo_path: Path) -> Path | None:
    candidate = Path(repo_path) / INFO_YAML_FILENAME
    return candidate if candidate.is_file() else None


def _person(entry: object, title: str) -> MaintainerInfoItem | None:
    if not isinstance(entry, dict):
        return None
    name = entry.get("name")
    email = entry.get("email")
    if not name and not email:
        return None
    return MaintainerInfoItem(
        name=str(name) if name else None,
        email=str(email).strip() if email else None,
        title=title,
        normalized_title="maintainer",
    )


def parse_info_yaml(content: str) -> list[MaintainerInfoItem] | None:
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError:
        return None

    if not isinstance(data, dict):
        return None

    committers = data.get("committers")
    project_lead = data.get("project_lead")
    if not isinstance(committers, list) or not isinstance(project_lead, dict):
        return None

    # The `tsc` section is a log of approved committer changes, removals included, so only
    # the lead and the current committers are read.
    people = [_person(project_lead, "Project Lead")]
    people.extend(_person(entry, "Committer") for entry in committers)

    maintainers: list[MaintainerInfoItem] = []
    seen: set[str] = set()
    for person in people:
        if person is None:
            continue
        key = (person.email or person.name or "").lower()
        if key in seen:
            continue
        seen.add(key)
        maintainers.append(person)

    # An empty result here means malformed entries, not an empty roster: accepting it would
    # end-date every current maintainer.
    return maintainers or None
