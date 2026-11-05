export enum MemberMergeSuggestionTable {
  MEMBER_TO_MERGE_FILTERED = 'memberToMerge',
  MEMBER_TO_MERGE_RAW = 'memberToMergeRaw',
}

export enum OrganizationMergeSuggestionTable {
  ORGANIZATION_TO_MERGE_FILTERED = 'organizationToMerge',
  ORGANIZATION_TO_MERGE_RAW = 'organizationToMergeRaw',
}

export enum MergeSuggestionSource {
  GLOBAL_SIMILARITY = 'global-similarity',
  SUBPROJECT_SIMILARITY = 'subproject-similarity',
  IDENTITY_CONFLICT = 'identity-conflict',
  ENRICHMENT_CONFLICT = 'enrichment-conflict',
}
