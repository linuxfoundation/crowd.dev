from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import crowdgit.services.maintainer.maintainer_service as maintainer_service_module
from crowdgit.services.maintainer.maintainer_service import MaintainerService

CNCF_YAML = """
maintainers:
  - project_id: cri-o
    org: cri-o
    teams:
      - name: maintainers
        managed: true
        members:
          - alice
          - bob
"""


@pytest.fixture
def cncf_repo(tmp_path: Path) -> Path:
    (tmp_path / "maintainers.yaml").write_text(CNCF_YAML)
    (tmp_path / "CODEOWNERS").write_text("* @alice @bob @charlie\n")
    return tmp_path


@pytest.mark.asyncio
async def test_extract_maintainers_bypasses_detection_for_cncf_repo(cncf_repo: Path):
    service = MaintainerService()

    result = await service.extract_maintainers(
        str(cncf_repo),
        saved_maintainer_file=None,
        repo_url="https://github.com/cri-o/.project",
    )

    assert result.maintainer_file == "maintainers.yaml"
    assert {m.github_username for m in result.maintainer_info} == {"alice", "bob"}


@pytest.mark.asyncio
async def test_extract_maintainers_self_corrects_when_stuck_on_wrong_file(cncf_repo: Path):
    service = MaintainerService()

    result = await service.extract_maintainers(
        str(cncf_repo),
        saved_maintainer_file="CODEOWNERS",
        repo_url="https://github.com/cri-o/.project",
    )

    assert result.maintainer_file == "maintainers.yaml"
    assert {m.github_username for m in result.maintainer_info} == {"alice", "bob"}


@pytest.mark.asyncio
async def test_extract_maintainers_falls_back_when_not_cncf_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    (tmp_path / "maintainers.yaml").write_text(CNCF_YAML)
    service = MaintainerService()
    monkeypatch.setattr(
        service, "classify_candidates_with_ai", AsyncMock(return_value=(set(), 0.0))
    )
    monkeypatch.setattr(
        service, "find_maintainer_file_with_ai", AsyncMock(return_value=(None, 0.0))
    )

    result = await service.extract_maintainers(
        str(tmp_path),
        saved_maintainer_file=None,
        repo_url="https://github.com/cri-o/cri-o",
    )

    assert result.not_found is True


@pytest.mark.asyncio
async def test_extract_maintainers_falls_back_when_cncf_parsing_raises(
    cncf_repo: Path, monkeypatch: pytest.MonkeyPatch
):
    service = MaintainerService()
    monkeypatch.setattr(
        maintainer_service_module,
        "parse_cncf_maintainers_yaml",
        lambda content: (_ for _ in ()).throw(ValueError("boom")),
    )
    monkeypatch.setattr(
        service, "classify_candidates_with_ai", AsyncMock(return_value=(set(), 0.0))
    )
    monkeypatch.setattr(
        service, "find_maintainer_file_with_ai", AsyncMock(return_value=(None, 0.0))
    )

    result = await service.extract_maintainers(
        str(cncf_repo),
        saved_maintainer_file=None,
        repo_url="https://github.com/cri-o/.project",
    )

    assert result.not_found is True
