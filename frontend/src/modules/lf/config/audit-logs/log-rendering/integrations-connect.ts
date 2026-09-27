import type { LogRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering/index';
import { lfIdentities } from '@/config/identities';

const integrationsConnect: LogRenderingConfig<{ platform?: string }> = {
  label: 'Integration connected',
  changes: () => null,
  description: (log) => {
    const integration = lfIdentities[log.newState?.platform || log.oldState?.platform || ''];
    if (integration) {
      return `Integration: ${integration.name}`;
    }
    return '';
  },
  properties: (log) => {
    const integration = lfIdentities[log.newState?.platform || log.oldState?.platform || ''];
    if (integration) {
      return [{
        label: 'Integration',
        value: integration.name,
      }];
    }
    return [];
  },
};

export default integrationsConnect;
