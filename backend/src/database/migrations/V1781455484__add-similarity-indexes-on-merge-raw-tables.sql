-- Supports similarity range filtering used by
-- getRawMemberSuggestions / getRawOrganizationSuggestions.
create index concurrently if not exists "ix_memberToMergeRaw_similarity"
  on "memberToMergeRaw" (similarity);

create index concurrently if not exists "ix_organizationToMergeRaw_similarity"
  on "organizationToMergeRaw" (similarity);
