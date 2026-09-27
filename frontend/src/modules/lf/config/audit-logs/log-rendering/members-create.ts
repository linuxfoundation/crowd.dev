import type { LogRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering/index';

// Member snapshot written by MemberRepository.create and the public createMember API.
interface MemberCreateState {
  displayName?: string;
}

const membersCreate: LogRenderingConfig<unknown, MemberCreateState> = {
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
