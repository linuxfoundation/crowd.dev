from pydantic import BaseModel, Field


class CloneBatchInfo(BaseModel):
    """Model for clone batch information during repository cloning operations"""

    repo_path: str | None = Field(None, description="Local repository path")
    remote: str = Field(..., description="Remote repository URL")
    is_final_batch: bool = Field(default=False, description="Whether this is the final batch")
    is_first_batch: bool = Field(default=True, description="Whether this is the first batch")
    latest_commit_in_repo: str | None = Field(
        None, description="Hash of the latest commit in repo"
    )
    clone_with_batches: bool = Field(
        default=True, description="Whether repo is cloned with batches"
    )

    class Config:
        """Pydantic configuration"""

        from_attributes = True
