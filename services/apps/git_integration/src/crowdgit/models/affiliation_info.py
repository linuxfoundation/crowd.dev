from pydantic import BaseModel


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
