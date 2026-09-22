from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from crowdmail.enums import ListPriority, ListState


class MailingList(BaseModel):
    """Mailing list model, joining mailinglist.lists + mailinglist.listProcessing"""

    id: str = Field(..., description="List ID")
    name: str = Field(..., description="List name")
    source_url: str = Field(..., description="Public-inbox/lore source URL")
    segment_id: str = Field(..., description="Segment ID")
    integration_id: str = Field(..., description="Integration ID")
    state: ListState = Field(default=ListState.PENDING, description="Processing state")
    priority: int = Field(default=ListPriority.NORMAL, description="Processing priority")
    locked_at: datetime | None = Field(None, description="Timestamp when list was locked")
    last_processed_at: datetime | None = Field(None, description="Last processing timestamp")
    last_processed_heads: dict[str, str] = Field(
        default_factory=dict, description="Map of shard index to last processed commit SHA"
    )

    @classmethod
    def from_db(cls, db_data: dict[str, Any]) -> MailingList:
        """Create MailingList instance from database data"""
        list_data = db_data.copy()

        for key, value in list_data.items():
            if value is not None and isinstance(value, uuid.UUID):
                list_data[key] = str(value)

        field_mapping = {
            "sourceUrl": "source_url",
            "segmentId": "segment_id",
            "integrationId": "integration_id",
            "lockedAt": "locked_at",
            "lastProcessedAt": "last_processed_at",
            "lastProcessedHeads": "last_processed_heads",
        }
        for db_field, model_field in field_mapping.items():
            if db_field in list_data:
                list_data[model_field] = list_data.pop(db_field)

        return cls(**list_data)

    class Config:
        """Pydantic configuration"""

        from_attributes = True
        json_encoders = {datetime: lambda v: v.isoformat() if v else None}
