export enum OrganizationAttributeName {
  SOURCE_ID = 'sourceId',
  URL = 'url',
  SYNC_REMOTE = 'syncRemote',
  DOMAIN = 'domain',
}

export enum OrganizationSource {
  EMAIL_DOMAIN = 'email-domain',
  ENRICHMENT_PROGAI = 'enrichment-progai',
  ENRICHMENT_CLEARBIT = 'enrichment-clearbit',
  ENRICHMENT_CRUSTDATA = 'enrichment-crustdata',
  GITHUB = 'github',
  UI = 'ui',
  CVENT = 'cvent',
  TNC = 'tnc',
}

export enum OrganizationMergeSuggestionType {
  BY_IDENTITY = 'by_identity',
}

export enum OrganizationAttributeType {
  STRING = 'string',
  BOOLEAN = 'boolean',
  INTEGER = 'integer',
  DECIMAL = 'decimal',
  OBJECT = 'object',
}

export enum OrganizationIdentityType {
  USERNAME = 'username',
  PRIMARY_DOMAIN = 'primary-domain',
  ALTERNATIVE_DOMAIN = 'alternative-domain',
  AFFILIATED_PROFILE = 'affiliated-profile',
  EMAIL = 'email',
}

export enum OrganizationAttributeSource {
  CUSTOM = 'custom',
  ENRICHMENT_LFX_INTERNAL_API = 'enrichment-lfx-internal-api',
  ENRICHMENT_PEOPLEDATALABS = 'enrichment-peopledatalabs',
  CVENT = 'cvent',
  TNC = 'tnc',
  // legacy - keeping this for backward compatibility
  ENRICHMENT = 'enrichment',
  GITHUB = 'github',
  EMAIL = 'email',
}
