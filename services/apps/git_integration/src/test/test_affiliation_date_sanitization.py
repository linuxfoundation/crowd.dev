from datetime import date

import orjson

from crowdgit.models.affiliation_info import (
    AffiliationContributor,
    AffiliationContributorEntry,
    AffiliationOrganizationStint,
    RepoAffiliationRegistry,
)


def test_stint_nulls_inverted_date_range():
    stint = AffiliationOrganizationStint(
        domain="example.com",
        date_start=date(2025, 8, 11),
        date_end=date(2022, 9, 20),
    )

    assert stint.date_start is None
    assert stint.date_end is None


def test_stint_nulls_end_without_start():
    stint = AffiliationOrganizationStint(
        domain="example.com",
        date_end=date(2022, 9, 20),
    )

    assert stint.date_start is None
    assert stint.date_end is None


def test_stint_keeps_valid_date_range():
    stint = AffiliationOrganizationStint(
        domain="example.com",
        date_start=date(2019, 6, 24),
        date_end=date(2022, 9, 20),
    )

    assert stint.date_start == date(2019, 6, 24)
    assert stint.date_end == date(2022, 9, 20)


def test_snapshot_load_sanitizes_inverted_dates():
    snapshot = [
        {
            "contributor": {"email": "user@example.com"},
            "organizations": [
                {
                    "domain": "example.com",
                    "dateStart": "2025-08-11",
                    "dateEnd": "2022-09-20",
                }
            ],
        }
    ]

    registry = RepoAffiliationRegistry.from_db(
        {
            "repoId": "00000000-0000-0000-0000-000000000001",
            "status": "success",
            "snapshot": orjson.dumps(snapshot).decode(),
        }
    )

    assert registry.snapshot is not None
    stint = registry.snapshot[0].organizations[0]
    assert stint.date_start is None
    assert stint.date_end is None


def test_snapshot_load_keeps_valid_dates():
    snapshot = [
        AffiliationContributorEntry(
            contributor=AffiliationContributor(email="user@example.com"),
            organizations=[
                AffiliationOrganizationStint(
                    domain="example.com",
                    date_start=date(2019, 6, 24),
                    date_end=date(2022, 9, 20),
                )
            ],
        )
    ]

    registry = RepoAffiliationRegistry(
        repo_id="00000000-0000-0000-0000-000000000001",
        status="success",
        snapshot=snapshot,
    )

    serialized = registry.snapshot_for_db()
    loaded = RepoAffiliationRegistry.from_db(
        {
            "repoId": "00000000-0000-0000-0000-000000000001",
            "status": "success",
            "snapshot": serialized,
        }
    )

    stint = loaded.snapshot[0].organizations[0]
    assert stint.date_start == date(2019, 6, 24)
    assert stint.date_end == date(2022, 9, 20)
