from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from crowdgit.services.maintainer.info_yaml_maintainers import parse_info_yaml
from crowdgit.services.maintainer.maintainer_service import MaintainerService

INFO_YAML = """
---
project: 'vpp'
project_creation_date: '2015-12-08'
project_category: ''
lifecycle_state: 'Incubation'
project_lead: &vpp_ptl
    name: 'Damjan Marion'
    email: 'dmarion@me.com'
    id: 'dmarion'
    company: 'cisco'
primary_contact: *vpp_ptl
issue_tracking:
    type: 'jira'
    url: 'https://jira.fd.io/projects/VPP'
    key: 'VPP'
mailing_list:
    type: 'groups.io'
    url: 'https://lists.fd.io/g/vpp-dev'
realtime_discussion: ''
repositories:
    - 'vpp'
committers:
    - <<: *vpp_ptl
    - name: 'Dave Barach'
      company: 'cisco'
      email: 'openvpp@barachs.net'
      id: 'dbarach'
    - name: 'Florin Coras'
      company: 'cisco'
      email: 'florin.coras@gmail.com'
      id: 'florin.coras'
tsc:
    approval: 'https://lists.fd.io/g/tsc'
    changes:
        - type: 'Removal'
          name: 'Former Committer'
          link: 'https://lists.fd.io/g/tsc/message/1'
"""


def test_parse_info_yaml_reads_lead_and_committers():
    maintainers = parse_info_yaml(INFO_YAML)

    assert [(m.name, m.email, m.title) for m in maintainers] == [
        ("Damjan Marion", "dmarion@me.com", "Project Lead"),
        ("Dave Barach", "openvpp@barachs.net", "Committer"),
        ("Florin Coras", "florin.coras@gmail.com", "Committer"),
    ]
    assert {m.normalized_title for m in maintainers} == {"maintainer"}
    assert all(m.github_username is None for m in maintainers)


def test_parse_info_yaml_skips_tsc_change_log():
    names = {m.name for m in parse_info_yaml(INFO_YAML)}

    assert "Former Committer" not in names


@pytest.mark.parametrize(
    "content",
    [
        "project: 'vpp'\nmaintainers:\n  - alice\n",
        "project_lead: {name: 'A'}\ncommitters: 'not a list'\n",
        "committers: []\n",
        "- just\n- a list\n",
        "key: [unclosed\n",
    ],
)
def test_parse_info_yaml_rejects_other_formats(content: str):
    assert parse_info_yaml(content) is None


@pytest.fixture
def gerrit_repo(tmp_path: Path) -> Path:
    (tmp_path / "INFO.yaml").write_text(INFO_YAML)
    (tmp_path / "AUTHORS").write_text("Someone Else <someone@example.com>\n")
    return tmp_path


@pytest.mark.asyncio
async def test_extract_maintainers_uses_info_yaml(gerrit_repo: Path):
    service = MaintainerService()

    result = await service.extract_maintainers(
        str(gerrit_repo),
        saved_maintainer_file=None,
        repo_url="https://gerrit.fd.io/r/vpp",
    )

    assert result.maintainer_file == "INFO.yaml"
    assert len(result.maintainer_info) == 3
    assert result.total_cost == 0


@pytest.mark.asyncio
async def test_extract_maintainers_switches_from_saved_file_to_info_yaml(gerrit_repo: Path):
    service = MaintainerService()

    result = await service.extract_maintainers(
        str(gerrit_repo),
        saved_maintainer_file="AUTHORS",
        repo_url="https://gerrit.fd.io/r/vpp",
    )

    assert result.maintainer_file == "INFO.yaml"


@pytest.mark.asyncio
async def test_extract_maintainers_falls_back_when_info_yaml_is_not_lf_format(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    (tmp_path / "INFO.yaml").write_text("name: some-tool\nversion: 1.0\n")
    service = MaintainerService()
    monkeypatch.setattr(service, "_ripgrep_search", AsyncMock(return_value=[]))
    monkeypatch.setattr(service, "_list_repo_files", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        service, "classify_candidates_with_ai", AsyncMock(return_value=(set(), 0.0))
    )
    monkeypatch.setattr(
        service, "find_maintainer_file_with_ai", AsyncMock(return_value=(None, 0.0))
    )

    result = await service.extract_maintainers(
        str(tmp_path),
        saved_maintainer_file=None,
        repo_url="https://github.com/example/tool",
    )

    assert result.not_found is True
