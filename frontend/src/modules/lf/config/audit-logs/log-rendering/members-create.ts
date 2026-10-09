import type { LogRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering/index';

const membersCreate: LogRenderingConfig<unknown, { displayName?: string }> = {
  label: 'Profile created',
  changes: () => null,
  description: (log) => {
    const member = log.newState?.displayName;

    if (member) {
      return `${member}<br>ID: ${log.entityId}`;
    }

    return '';
  },
  properties: (log) => {
    const member = log.newState?.displayName;

    if (member) {
      return [{
        label: 'Profile',
        value: `${member}<br><span>ID: ${log.entityId}</span>`,
      }];
    }

    return [];
  },
};

export default membersCreate;
