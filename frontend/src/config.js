/**
 * Tenant Mode
 * multi: Allow new users to create new tenants.
 * multi-with-subdomain: Same as multi, but enable access to the tenant via subdomain.
 * single: One tenant, the first user to register will be the admin.
 */
const tenantMode = 'multi';

/**
 * Plan payments configuration.
 */
const isPlanEnabled = true;

const defaultConfig = {
  frontendUrl: {
    host: import.meta.env.VUE_APP_FRONTEND_HOST,
    protocol: import.meta.env.VUE_APP_FRONTEND_PROTOCOL,
  },
  backendUrl: import.meta.env.VUE_APP_BACKEND_URL,
  websocketsUrl: import.meta.env.VUE_APP_WEBSOCKETS_URL,
  tenantMode,
  isPlanEnabled,
  gitHubInstallationUrl: import.meta.env.VUE_APP_GITHUB_INSTALLATION_URL,
  discordInstallationUrl: import.meta.env.VUE_APP_DISCORD_INSTALLATION_URL,
  edition: import.meta.env.VUE_APP_EDITION,
  communityPremium: import.meta.env.VUE_APP_COMMUNITY_PREMIUM,
  env: import.meta.env.VUE_APP_ENV,
  hotjarKey: import.meta.env.VUE_APP_HOTJAR_KEY,
  nangoUrl: import.meta.env.VUE_APP_NANGO_URL,
  stripe: {
    publishableKey: import.meta.env.VUE_APP_STRIPE_PUBLISHABLE_KEY || '',
    growthPlanPaymentLink:
      import.meta.env.VUE_APP_STRIPE_GROWTH_PLAN_PAYMENT_LINK || '',
    customerPortalLink:
      import.meta.env.VUE_APP_STRIPE_CUSTOMER_PORTAL_LINK || '',
  },
  auth0: {
    domain: import.meta.env.VUE_APP_AUTH0_DOMAIN,
    clientId: import.meta.env.VUE_APP_AUTH0_CLIENT_ID,
    database: import.meta.env.VUE_APP_AUTH0_DATABASE,
  },
  lf: {
    tenantId: import.meta.env.VUE_APP_LF_TENANT_ID,
  },
  isGitEnabled: import.meta.env.VUE_APP_IS_GIT_ENABLED,
  isGroupsioEnabled: import.meta.env.VUE_APP_IS_GROUPSIO_ENABLED,
  isConfluenceEnabled: import.meta.env.VUE_APP_IS_CONFLUENCE_ENABLED,
  isGerritEnabled: import.meta.env.VUE_APP_IS_GERRIT_ENABLED,
  isTwitterEnabled: import.meta.env.VUE_APP_IS_TWITTER_ENABLED,
  datadog: {
    rum: {
      applicationId: import.meta.env.VUE_APP_DATADOG_RUM_APPLICATION_ID,
      clientToken: import.meta.env.VUE_APP_DATADOG_RUM_CLIENT_TOKEN,
    },
  },
  permissions: {
    teamUserIds: import.meta.env.VUE_APP_TEAM_USER_IDS,
  },
};

const composedConfig = {
  frontendUrl: {
    host: 'CROWD_VUE_APP_FRONTEND_HOST',
    protocol: 'CROWD_VUE_APP_FRONTEND_PROTOCOL',
  },
  backendUrl: 'CROWD_VUE_APP_BACKEND_URL',
  websocketsUrl: 'CROWD_VUE_APP_WEBSOCKETS_URL',
  tenantMode,
  isPlanEnabled,
  gitHubInstallationUrl: 'CROWD_VUE_APP_GITHUB_INSTALLATION_URL',
  discordInstallationUrl: 'CROWD_VUE_APP_DISCORD_INSTALLATION_URL',
  edition: 'CROWD_VUE_APP_EDITION',
  communityPremium: 'CROWD_VUE_APP_COMMUNITY_PREMIUM',
  env: 'CROWD_VUE_APP_ENV',
  hotjarKey: 'CROWD_VUE_APP_HOTJAR_KEY',
  nangoUrl: 'CROWD_VUE_APP_NANGO_URL',
  typeformId: 'CROWD_VUE_APP_TYPEFORM_ID',
  typeformTitle: 'CROWD_VUE_APP_TYPEFORM_TITLE',
  stripe: {
    publishableKey: 'CROWD_VUE_APP_STRIPE_PUBLISHABLE_KEY' || '',
    growthPlanPaymentLink:
      'CROWD_VUE_APP_STRIPE_GROWTH_PLAN_PAYMENT_LINK' || '',
    customerPortalLink: 'CROWD_VUE_APP_STRIPE_CUSTOMER_PORTAL_LINK' || '',
  },
  auth0: {
    domain: 'CROWD_VUE_APP_AUTH0_DOMAIN',
    clientId: 'CROWD_VUE_APP_AUTH0_CLIENT_ID',
    database: 'CROWD_VUE_APP_AUTH0_DATABASE',
  },
  lf: {
    tenantId: 'CROWD_VUE_APP_LF_TENANT_ID',
  },
  isGitEnabled: 'CROWD_VUE_APP_IS_GIT_ENABLED',
  isGroupsioEnabled: 'CROWD_VUE_APP_IS_GROUPSIO_ENABLED',
  isTwitterEnabled: 'CROWD_VUE_APP_IS_TWITTER_ENABLED',
  isConfluenceEnabled: 'CROWD_VUE_APP_IS_CONFLUENCE_ENABLED',
  isGerritEnabled: 'CROWD_VUE_APP_IS_GERRIT_ENABLED',
  datadog: {
    rum: {
      applicationId: 'CROWD_VUE_APP_DATADOG_RUM_APPLICATION_ID',
      clientToken: 'CROWD_VUE_APP_DATADOG_RUM_CLIENT_TOKEN',
    },
  },
  permissions: {
    teamUserIds: 'CROWD_VUE_APP_TEAM_USER_IDS',
  },
};

const config = defaultConfig.backendUrl ? defaultConfig : composedConfig;

config.isCommunityVersion = config.edition === 'community';
config.hasPremiumModules = !config.isCommunityVersion || config.communityPremium === 'true';
config.isGitIntegrationEnabled = config.isGitEnabled === 'true';
config.isGroupsioIntegrationEnabled = config.isGroupsioEnabled === 'true';
config.isTwitterIntegrationEnabled = config.isTwitterEnabled === 'true';
config.isConfluenceEnabled = config.isConfluenceEnabled === 'true';

export default config;
