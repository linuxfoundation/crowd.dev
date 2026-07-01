from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import orjson
from loguru import logger
from pydantic import BaseModel, TypeAdapter, ValidationError


class AffiliationContributor(BaseModel):
    email: str | None = None
    name: str | None = None
    github: str | None = None


class AffiliationOrganization(BaseModel):
    name: str | None = None
    domain: str | None = None


class AffiliationInfoItem(BaseModel):
    contributor: AffiliationContributor
    organization: AffiliationOrganization


class AffiliationFile(BaseModel):
    file_name: str | None = None
    error: str | None = None


class AffiliationParseOutput(BaseModel):
    affiliations: list[AffiliationInfoItem] | None = None
    error: str | None = None


_SNAPSHOT_ADAPTER = TypeAdapter(list[AffiliationInfoItem])


class RepoAffiliationRegistry(BaseModel):
    repo_id: str
    file_path: str | None = None
    file_hash: str | None = None
    status: str
    snapshot: list[AffiliationInfoItem] | None = None
    last_run_at: datetime | None = None

    @classmethod
    def from_db(cls, db_data: dict[str, Any]) -> RepoAffiliationRegistry:
        row = db_data.copy()

        for key, value in row.items():
            if value is not None and isinstance(value, uuid.UUID):
                row[key] = str(value)

        field_mapping = {
            "repoId": "repo_id",
            "filePath": "file_path",
            "fileHash": "file_hash",
            "lastRunAt": "last_run_at",
        }
        for db_field, model_field in field_mapping.items():
            if db_field in row:
                row[model_field] = row.pop(db_field)

        snapshot = row.get("snapshot")
        if snapshot is not None:
            row["snapshot"] = cls._parse_snapshot(snapshot)

        return cls(**row)

    @staticmethod
    def _parse_snapshot(snapshot) -> list[AffiliationInfoItem] | None:
        if isinstance(snapshot, str | bytes):
            try:
                snapshot = orjson.loads(snapshot)
            except orjson.JSONDecodeError as error:
                logger.warning(
                    f"Invalid affiliation snapshot JSON in registry, will re-parse: {error}"
                )
                return None
        if isinstance(snapshot, dict) and "affiliations" in snapshot:
            snapshot = snapshot["affiliations"]
        try:
            return _SNAPSHOT_ADAPTER.validate_python(snapshot)
        except ValidationError as error:
            logger.warning(f"Invalid affiliation snapshot in registry, will re-parse: {error}")
            return None

    def snapshot_for_db(self) -> str | None:
        if self.snapshot is None:
            return None
        return orjson.dumps([item.model_dump() for item in self.snapshot]).decode()
