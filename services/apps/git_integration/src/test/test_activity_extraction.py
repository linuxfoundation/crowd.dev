"""
Test commit and activity extraction from real git repository.

This test validates that CommitService correctly extracts commits and activities
from a real git repository and produces the expected output.
"""

import json
import os
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Import crowdgit modules (environment variables are set in conftest.py)
from crowdgit.models import CloneBatchInfo, Repository
from crowdgit.services.commit.commit_service import CommitService
from crowdgit.services.queue.queue_service import QueueService

# Paths
FIXTURES_DIR = Path(__file__).parent / "fixtures"
REPOS_DIR = Path(__file__).parent / "repos"
OUTPUTS_DIR = Path(__file__).parent / "outputs"
CUSTOM_OUTPUTS_DIR = OUTPUTS_DIR / "custom"

# Ensure directories exist
CUSTOM_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# Allow specifying different repo via environment variable (name only, looks in repos/)
repo_name = os.environ.get("TEST_REPO_NAME", "test-repo")
TEST_REPO_PATH = REPOS_DIR / repo_name

# Output files location: default repo in outputs/, custom repos in outputs/custom/
is_custom_repo = repo_name != "test-repo"
output_dir = CUSTOM_OUTPUTS_DIR if is_custom_repo else OUTPUTS_DIR

ACTUAL_OUTPUT_FILE = output_dir / f"{repo_name}_actual.json"

# Allow overriding expected file via environment variable
if os.environ.get("TEST_EXPECTED_FILE"):
    EXPECTED_OUTPUT_FILE = output_dir / os.environ["TEST_EXPECTED_FILE"]
else:
    EXPECTED_OUTPUT_FILE = output_dir / f"{repo_name}_expected.json"

SEED_FILE = FIXTURES_DIR / "test_repo_seed.json"


def ensure_test_repo_exists():
    """Ensure test repository exists, build it if not."""
    if not TEST_REPO_PATH.exists() or not (TEST_REPO_PATH / ".git").exists():
        print("Test repository not found, building from seed...")
        build_script = FIXTURES_DIR / "build_test_repo.py"
        subprocess.run(["python3", str(build_script)], check=True, cwd=str(FIXTURES_DIR))
        print(f"✅ Test repository built at {TEST_REPO_PATH}")
    else:
        print(f"✅ Test repository found at {TEST_REPO_PATH}")


def load_expected_activities() -> list[dict[str, Any]]:
    """Load expected activities from JSON file."""
    if not EXPECTED_OUTPUT_FILE.exists():
        raise FileNotFoundError(
            f"Expected activities file not found: {EXPECTED_OUTPUT_FILE}\n"
            "Please create this file with expected test output."
        )

    with open(EXPECTED_OUTPUT_FILE, "r") as f:
        return json.load(f)


@pytest.fixture
def mock_queue_service():
    """Mock QueueService for testing."""
    mock_service = Mock(spec=QueueService)
    mock_service.send_batch_activities = AsyncMock()
    return mock_service


@pytest.fixture
def commit_service(mock_queue_service):
    """Create CommitService instance for testing."""
    return CommitService(queue_service=mock_queue_service)


@pytest.fixture
def test_repository():
    """Create test Repository model."""
    from datetime import datetime

    return Repository(
        id="test-repo-id-123",
        url="https://github.com/test/repo.git",
        segment_id="test-segment-id",
        integration_id="test-integration-id",
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )


@pytest.fixture
def batch_info():
    """Create CloneBatchInfo for testing."""
    return CloneBatchInfo(
        repo_path=str(TEST_REPO_PATH),
        remote="https://github.com/test/repo.git",
        is_first_batch=True,
        is_final_batch=True,
        clone_with_batches=False,
        latest_commit_in_repo=None,  # Will be set by service
        prev_batch_edge_commit=None,
        edge_commit=None,
    )


@pytest.mark.asyncio
class TestCommitExtraction:
    """Test suite for commit and activity extraction."""

    async def test_extract_all_commits_and_activities(
        self, commit_service, test_repository, batch_info, mock_queue_service
    ):
        """
        Test that all commits and activities are correctly extracted from test repository.

        This test:
        1. Ensures test repository exists
        2. Processes all commits using CommitService
        3. Captures activities that would be inserted to DB
        4. Compares with expected output
        """
        # Ensure test repo exists
        ensure_test_repo_exists()

        # Mock database operations to capture activities
        captured_activities_db = []

        async def mock_batch_insert(activities):
            """Capture activities that would be inserted to DB."""
            captured_activities_db.extend(activities)

        async def mock_save_execution(execution):
            """Mock service execution save."""
            pass

        with patch(
            "crowdgit.services.commit.commit_service.batch_insert_activities", mock_batch_insert
        ):
            with patch(
                "crowdgit.services.commit.commit_service.save_service_execution",
                mock_save_execution,
            ):
                # Process commits
                await commit_service.process_batch_commits(
                    repository=test_repository, batch_info=batch_info
                )

        # Verify activities were extracted
        assert len(captured_activities_db) > 0, "No activities were extracted"

        print("\n📊 Extraction Results:")
        print(f"   DB Activities extracted: {len(captured_activities_db)}")

        # Parse DB activities to readable format
        # DB format: (result_id, state, json_data, tenant_id, integration_id)
        parsed_activities = []
        for activity_tuple in captured_activities_db:
            result_id, state, json_data, tenant_id, integration_id = activity_tuple
            data = json.loads(json_data)
            parsed_activities.append(data["data"])  # Extract the actual activity from data wrapper

        # Sort activities for consistent comparison
        def activity_sort_key(activity):
            """Generate a sort key for stable activity ordering."""
            return (
                activity.get("sourceId", ""),
                activity.get("type", ""),
                activity.get("timestamp", ""),
                str(activity.get("member", {}).get("identities", [{}])[0].get("value", "")),
            )

        parsed_activities.sort(key=activity_sort_key)

        # Save actual output for inspection
        with open(ACTUAL_OUTPUT_FILE, "w") as f:
            json.dump(parsed_activities, f, indent=2, default=str)
        print(f"💾 Saved actual output to: {ACTUAL_OUTPUT_FILE}")

        # Load and compare with expected activities
        if not EXPECTED_OUTPUT_FILE.exists():
            print(f"\n❌ No expected baseline found at: {EXPECTED_OUTPUT_FILE}")
            print(f"   Actual output saved to: {ACTUAL_OUTPUT_FILE}")
            print(f"   To create baseline: cp {ACTUAL_OUTPUT_FILE} {EXPECTED_OUTPUT_FILE}")
            pytest.fail(
                f"Expected baseline not found: {EXPECTED_OUTPUT_FILE}\n"
                f"Create baseline with: cp {ACTUAL_OUTPUT_FILE} {EXPECTED_OUTPUT_FILE}"
            )

        expected_activities = load_expected_activities()

        if len(expected_activities) == 0:
            print("\n❌ Expected activities file is empty")
            print(f"   Review {ACTUAL_OUTPUT_FILE}")
            print(f"   Copy to: {EXPECTED_OUTPUT_FILE}")
            pytest.fail("Expected baseline file is empty")

        # Sort expected activities using the same key
        expected_activities.sort(key=activity_sort_key)

        # Compare with expected output
        assert len(parsed_activities) == len(expected_activities), (
            f"Expected {len(expected_activities)} activities, got {len(parsed_activities)}"
        )

        print(f"✅ Activity count matches: {len(parsed_activities)}")

        # Deep comparison - compare EVERYTHING
        for i, (actual, expected) in enumerate(
            zip(parsed_activities, expected_activities, strict=False)
        ):
            if actual != expected:
                # Find the differences for detailed error message
                print(f"\n❌ Activity {i} mismatch:")
                print(f"Expected:\n{json.dumps(expected, indent=2)}")
                print(f"Actual:\n{json.dumps(actual, indent=2)}")

                # Show specific field differences
                for key in set(list(actual.keys()) + list(expected.keys())):
                    if actual.get(key) != expected.get(key):
                        print(
                            f"  Field '{key}': expected {expected.get(key)}, got {actual.get(key)}"
                        )

            assert actual == expected, (
                f"Activity {i}: Complete structure mismatch (see details above)"
            )

        print("✅ All activities match expected output (complete deep validation)")

    async def test_activity_types_coverage(
        self, commit_service, test_repository, batch_info, mock_queue_service
    ):
        """
        Test that different activity types are correctly extracted.

        Verifies that the test repository contains commits with various activity types:
        - authored-commit
        - commited-commit
        - signed-off-commit
        - reviewed-commit
        - tested-commit
        - co-authored-commit

        Note: This test only runs for the default test-repo, not custom repos.
        """
        # Skip this test if using a custom repository
        if os.environ.get("TEST_REPO_NAME"):
            pytest.skip("Skipping activity types coverage test for custom repository")

        ensure_test_repo_exists()

        # Capture DB activities
        captured_activities_db = []

        async def mock_batch_insert(activities):
            captured_activities_db.extend(activities)

        async def mock_save_execution(execution):
            pass

        with patch(
            "crowdgit.services.commit.commit_service.batch_insert_activities", mock_batch_insert
        ):
            with patch(
                "crowdgit.services.commit.commit_service.save_service_execution",
                mock_save_execution,
            ):
                await commit_service.process_batch_commits(
                    repository=test_repository, batch_info=batch_info
                )

        # Extract activity types from DB records
        # DB format: (result_id, state, json_data, tenant_id, integration_id)
        activity_types = set()
        for activity_tuple in captured_activities_db:
            result_id, state, json_data, tenant_id, integration_id = activity_tuple
            data = json.loads(json_data)
            activity = data["data"]
            activity_types.add(activity["type"])

        print(f"\n📋 Activity types found: {sorted(activity_types)}")

        # Verify expected activity types are present
        expected_types = {
            "authored-commit",
            "committed-commit",  # Note: correct spelling (not "commited")
            "signed-off-commit",
            "reviewed-commit",
            "tested-commit",
            "co-authored-commit",
        }

        for expected_type in expected_types:
            assert expected_type in activity_types, (
                f"Expected activity type '{expected_type}' not found in extracted activities"
            )

        print("✅ All expected activity types found")


def test_seed_file_exists():
    """Test that seed file exists and is valid JSON."""
    assert SEED_FILE.exists(), f"Seed file not found: {SEED_FILE}"

    with open(SEED_FILE, "r") as f:
        seed_data = json.load(f)

    assert "commits" in seed_data, "Seed file must contain 'commits' key"
    assert len(seed_data["commits"]) > 0, "Seed file must contain at least one commit"

    print(f"✅ Seed file is valid with {len(seed_data['commits'])} commits")


def test_build_script_exists():
    """Test that build script exists and is executable."""
    build_script = FIXTURES_DIR / "build_test_repo.py"
    assert build_script.exists(), f"Build script not found: {build_script}"
    assert os.access(build_script, os.X_OK), f"Build script is not executable: {build_script}"

    print("✅ Build script found and executable")
