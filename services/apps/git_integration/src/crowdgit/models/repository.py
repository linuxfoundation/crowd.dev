from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from crowdgit.enums import RepositoryPriority, RepositoryState


class Repository(BaseModel):
    """Repository model"""

    id: str = Field(..., description="Repository ID")
    url: str = Field(..., description="Repository URL")
    segment_id: str | None = Field(None, description="Segment ID")
    integration_id: str | None = Field(None, description="Integration ID")
    state: RepositoryState = Field(default=RepositoryState.PENDING, description="Repository state")
    priority: int = Field(default=RepositoryPriority.NORMAL, description="Processing priority")
    last_processed_at: datetime | None = Field(None, description="Last processing timestamp")
    last_processed_commit: str | None = Field(None, description="Last processed commit hash")
    locked_at: datetime | None = Field(
        None, description="Timestamp when repository was locked for processing"
    )
    maintainer_file: str | None = Field(
        None, description="Name of the maintainer file found in repository"
    )
    last_maintainer_run_at: datetime | None = Field(
        None,
        description="Timestamp of when the repository maintainer processing was last executed",
    )
    branch: str | None = Field(
        None,
        description="The default branch being tracked for this repository (e.g., main, master, develop)",
    )
    forked_from: str | None = Field(
        None,
        description="The source repository URL if this repository is a fork",
    )
    parent_repo: Repository | None = Field(
        None, description="The parent repository (in case of fork) object from our database"
    )
    re_onboarding_count: int = Field(
        default=0,
        description="Tracks the number of times this repository has been re-onboarded. Used to identify unreachable commits via activity.attributes.cycle matching pattern onboarding-{reOnboardingCount}",
    )

    @classmethod
    def from_db(cls, db_data: dict[str, Any]) -> Repository:
        """Create Repository instance from database data"""
        # Convert database field names to model field names
        repo_data = db_data.copy()

        # Convert any UUID fields to strings
        for key, value in repo_data.items():
            if value is not None and isinstance(value, uuid.UUID):
                repo_data[key] = str(value)

        # Map database field names to model field names
        field_mapping = {
            "lastProcessedAt": "last_processed_at",
            "lastProcessedCommit": "last_processed_commit",
            "lockedAt": "locked_at",
            "segmentId": "segment_id",
            "gitIntegrationId": "integration_id",
            "maintainerFile": "maintainer_file",
            "lastMaintainerRunAt": "last_maintainer_run_at",
            "forkedFrom": "forked_from",
            "reOnboardingCount": "re_onboarding_count",
        }
        for db_field, model_field in field_mapping.items():
            if db_field in repo_data:
                repo_data[model_field] = repo_data.pop(db_field)

        return cls(**repo_data)

    class Config:
        """Pydantic configuration"""

        from_attributes = True
        json_encoders = {datetime: lambda v: v.isoformat() if v else None}


class RepositoryCreate(BaseModel):
    """Model for creating a new repository"""

    url: str = Field(..., description="Repository URL")
    segment_id: str | None = Field(None, description="Segment ID")
    integration_id: str | None = Field(None, description="Integration ID")
    priority: int = Field(default=RepositoryPriority.NORMAL, description="Processing priority")


class RepositoryResponse(BaseModel):
    """Response model for repository operations"""

    success: bool = Field(..., description="Operation success status")
    message: str = Field(..., description="Response message")
    data: Repository | None = Field(None, description="Repository data")
    error: str | None = Field(None, description="Error message if any")
