import { lfIntegrations } from '@/config/integrations';

const adminNotConnectablePlatforms = ['twitter'];

export const isAdminConnectable = (platform: string) => {
  const config = lfIntegrations()[platform];
  return !!config && !config.autoManaged && !adminNotConnectablePlatforms.includes(platform);
};
