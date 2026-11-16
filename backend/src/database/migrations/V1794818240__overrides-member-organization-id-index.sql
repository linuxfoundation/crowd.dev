-- Affiliation lookups join overrides on "memberOrganizationId" alone, which no index covered.
create index concurrently if not exists "ix_memberOrganizationAffiliationOverrides_memberOrganizationId"
  on "memberOrganizationAffiliationOverrides" ("memberOrganizationId");
