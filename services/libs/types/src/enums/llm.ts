export enum LlmModelType {
  CLAUDE_3_5_SONNET = 'anthropic.claude-3-5-sonnet-20240620-v1:0',
  CLAUDE_3_5_SONNET_V2 = 'anthropic.claude-3-5-sonnet-20241022-v2:0',
  CLAUDE_3_OPUS = 'anthropic.claude-3-opus-20240229-v1:0',
  CLAUDE_SONNET_4 = 'us.anthropic.claude-sonnet-4-20250514-v1:0',
  CLAUDE_HAIKU_4_5 = 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
}

export enum LlmQueryType {
  MEMBER_ENRICHMENT = 'member_enrichment',
  MEMBER_ENRICHMENT_FIND_RELATED_LINKEDIN_PROFILES = 'member_enrichment_find_related_linkedin_profiles',
  MEMBER_ENRICHMENT_SQUASH_MULTIPLE_VALUE_ATTRIBUTES = 'member_enrichment_squash_multiple_value_attributes',
  MEMBER_ENRICHMENT_SQUASH_WORK_EXPERIENCES_FROM_MULTIPLE_SOURCES = 'member_enrichment_squash_work_experiences_from_multiple_sources',
  MATCH_MAIN_GITHUB_ORGANIZATION_AND_DESCRIPTION = 'match_main_github_organization_and_description',
  REPO_CATEGORIES = 'repo_categories',
  REPO_COLLECTIONS = 'repo_collections',
  MEMBER_BOT_VALIDATION = 'member_bot_validation',
  SELECT_MOST_RELEVANT_DOMAIN = 'select_most_relevant_domain',
}
