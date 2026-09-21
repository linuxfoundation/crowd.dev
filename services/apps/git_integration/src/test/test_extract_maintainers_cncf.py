from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import crowdgit.services.maintainer.maintainer_service as maintainer_service_module
from crowdgit.database.crud import ProjectContext
from crowdgit.models import CloneBatchInfo, Repository
from crowdgit.models.maintainer_info import MaintainerInfoItem, MaintainerResult
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


# ---------------------------------------------------------------------------
# process_maintainers — project-level source gating
# ---------------------------------------------------------------------------

SIBLING_REPO_ID = "11111111-1111-1111-1111-111111111111"
PROJECT_REPO_ID = "22222222-2222-2222-2222-222222222222"
PROJECT_SEGMENT_ID = "33333333-3333-3333-3333-333333333333"


def _make_repository(repo_id: str, url: str) -> Repository:
    return Repository(id=repo_id, url=url, state="completed", priority=2)


def _make_batch(remote: str) -> CloneBatchInfo:
    return CloneBatchInfo(remote=remote, repo_path="/tmp/fake", is_first_batch=True)


def _project_context_with_sibling() -> ProjectContext:
    return ProjectContext(
        project_segment_id=PROJECT_SEGMENT_ID,
        project_repo_id=PROJECT_REPO_ID,
        project_repo_url="https://github.com/cri-o/.project",
        sibling_repo_ids=[SIBLING_REPO_ID],
    )


@pytest.mark.asyncio
async def test_process_maintainers_skips_sibling_when_project_repo_exists(
    monkeypatch: pytest.MonkeyPatch,
):
    """Non-.project repo is skipped when a .project sibling is already onboarded."""
    service = MaintainerService()
    repository = _make_repository(SIBLING_REPO_ID, "https://github.com/cri-o/cri-o")
    batch = _make_batch("https://github.com/cri-o/cri-o")

    extract_mock = AsyncMock()
    update_run_mock = AsyncMock()
    save_execution_mock = AsyncMock()

    monkeypatch.setattr(service, "check_if_interval_elapsed", AsyncMock(return_value=(True, 0.0)))
    monkeypatch.setattr(
        maintainer_service_module,
        "find_project_repo_sibling",
        AsyncMock(return_value=_project_context_with_sibling()),
    )
    monkeypatch.setattr(service, "extract_maintainers", extract_mock)
    monkeypatch.setattr(maintainer_service_module, "update_maintainer_run", update_run_mock)
    monkeypatch.setattr(maintainer_service_module, "save_service_execution", save_execution_mock)

    await service.process_maintainers(repository, batch)

    extract_mock.assert_not_called()
    update_run_mock.assert_called_once_with(SIBLING_REPO_ID, maintainer_file=None)
    execution = save_execution_mock.call_args[0][0]
    assert execution.error_code == "maintainer-skipped-project-level-source"


@pytest.mark.asyncio
async def test_process_maintainers_end_dates_siblings_after_project_repo_runs(
    monkeypatch: pytest.MonkeyPatch,
):
    """.project repo processing bulk end-dates all sibling maintainer rows."""
    service = MaintainerService()
    repository = _make_repository(PROJECT_REPO_ID, "https://github.com/cri-o/.project")
    batch = _make_batch("https://github.com/cri-o/.project")

    end_date_mock = AsyncMock()

    cncf_result = MaintainerResult(
        maintainer_file="maintainers.yaml",
        maintainer_info=[
            MaintainerInfoItem(
                github_username="alice", title="maintainer", normalized_title="maintainer"
            )
        ],
        cncf_authoritative=True,
    )

    monkeypatch.setattr(service, "check_if_interval_elapsed", AsyncMock(return_value=(True, 0.0)))
    monkeypatch.setattr(service, "extract_maintainers", AsyncMock(return_value=cncf_result))
    monkeypatch.setattr(service, "save_maintainers", AsyncMock())
    monkeypatch.setattr(maintainer_service_module, "update_maintainer_run", AsyncMock())
    monkeypatch.setattr(
        maintainer_service_module,
        "find_project_repo_sibling",
        AsyncMock(return_value=_project_context_with_sibling()),
    )
    monkeypatch.setattr(maintainer_service_module, "end_date_maintainers_for_repos", end_date_mock)
    monkeypatch.setattr(maintainer_service_module, "save_service_execution", AsyncMock())

    await service.process_maintainers(repository, batch)

    end_date_mock.assert_called_once()
    call_args = end_date_mock.call_args[0]
    assert call_args[0] == [SIBLING_REPO_ID]


@pytest.mark.asyncio
async def test_process_maintainers_runs_normally_when_no_project_repo(
    monkeypatch: pytest.MonkeyPatch,
):
    """Non-.project repo without a .project sibling runs full detection pipeline."""
    service = MaintainerService()
    repository = _make_repository(SIBLING_REPO_ID, "https://github.com/example/repo")
    batch = _make_batch("https://github.com/example/repo")

    extract_mock = AsyncMock(return_value=MaintainerResult(not_found=True))

    monkeypatch.setattr(service, "check_if_interval_elapsed", AsyncMock(return_value=(True, 0.0)))
    monkeypatch.setattr(
        maintainer_service_module,
        "find_project_repo_sibling",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(service, "extract_maintainers", extract_mock)
    monkeypatch.setattr(maintainer_service_module, "update_maintainer_run", AsyncMock())
    monkeypatch.setattr(maintainer_service_module, "save_service_execution", AsyncMock())

    await service.process_maintainers(repository, batch)

    extract_mock.assert_called_once()


@pytest.mark.asyncio
async def test_process_maintainers_end_dates_siblings_when_all_emeritus(
    monkeypatch: pytest.MonkeyPatch,
):
    """All-emeritus .project roster is still authoritative and end-dates siblings."""
    service = MaintainerService()
    repository = _make_repository(PROJECT_REPO_ID, "https://github.com/cri-o/.project")
    batch = _make_batch("https://github.com/cri-o/.project")

    end_date_mock = AsyncMock()

    cncf_result = MaintainerResult(
        maintainer_file="maintainers.yaml",
        maintainer_info=[],
        cncf_authoritative=True,
    )

    monkeypatch.setattr(service, "check_if_interval_elapsed", AsyncMock(return_value=(True, 0.0)))
    monkeypatch.setattr(service, "extract_maintainers", AsyncMock(return_value=cncf_result))
    monkeypatch.setattr(service, "save_maintainers", AsyncMock())
    monkeypatch.setattr(maintainer_service_module, "update_maintainer_run", AsyncMock())
    monkeypatch.setattr(
        maintainer_service_module,
        "find_project_repo_sibling",
        AsyncMock(return_value=_project_context_with_sibling()),
    )
    monkeypatch.setattr(maintainer_service_module, "end_date_maintainers_for_repos", end_date_mock)
    monkeypatch.setattr(maintainer_service_module, "save_service_execution", AsyncMock())

    await service.process_maintainers(repository, batch)

    end_date_mock.assert_called_once()
    call_args = end_date_mock.call_args[0]
    assert call_args[0] == [SIBLING_REPO_ID]


@pytest.mark.asyncio
async def test_save_maintainers_persists_emeritus_role(monkeypatch: pytest.MonkeyPatch):
    service = MaintainerService()
    item = MaintainerInfoItem(
        github_username="alice",
        name="Alice",
        title="Emeritus Maintainer",
        normalized_title="emeritus",
    )
    identity_id = "identity-123"

    monkeypatch.setattr(
        service,
        "_resolve_maintainers",
        AsyncMock(return_value=[(item, identity_id)]),
    )
    upsert_mock = AsyncMock()
    monkeypatch.setattr(maintainer_service_module, "upsert_maintainer", upsert_mock)

    await service.save_maintainers(
        repo_id="repo-1",
        repo_url="https://github.com/cri-o/.project",
        maintainers=[item],
        last_maintainer_run_at=None,
    )

    upsert_mock.assert_called_once()
    assert upsert_mock.call_args[0][3] == "emeritus"
