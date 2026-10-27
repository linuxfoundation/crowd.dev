import { lfIntegrations } from '@/config/integrations';

const adminNotConnectablePlatforms = ['twitter'];

export const isAdminConnectable = (platform: string) => {
  const config = lfIntegrations()[platform];
  return !!config && !config.autoManaged && !adminNotConnectablePlatforms.includes(platform);
};

export const adminConnectablePlatforms = () => {
  const keys = Object.keys(lfIntegrations()).filter(isAdminConnectable);
  return keys.includes('github') ? [...keys, 'github-nango'] : keys;
};
