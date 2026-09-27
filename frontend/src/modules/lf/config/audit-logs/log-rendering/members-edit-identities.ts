import type { LogRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering/index';
import { lfIdentities } from '@/config/identities';

// Platform -> usernames map this renderer reads; current backend writers capture identity arrays.
type MemberIdentitiesState = Record<string, string[]>;

const membersEditIdentities: LogRenderingConfig<MemberIdentitiesState> = {
  label: 'Profile identities updated',
  changes: (log) => {
    const removals = [];
    const additions = [];
    const changes = [];

    Object.keys(log.oldState).forEach((platform) => {
      log.oldState[platform].forEach((identity) => {
        if (!log.newState[platform] || log.newState[platform].length === 0 || !log.newState[platform].includes(identity)) {
          removals.push(`${lfIdentities[platform]?.name || platform} username: ${identity}`);
        }
      });
    });

    // Check for additions in newState
    Object.keys(log.newState).forEach((platform) => {
      log.newState[platform].forEach((identity) => {
        if (!log.oldState[platform] || !log.oldState[platform].includes(identity)) {
          additions.push(`${lfIdentities[platform]?.name || platform} username: ${identity}`);
        }
      });
    });

    return { removals, additions, changes };
  },
  description: (log) => `ID: ${log.entityId}`,
  properties: (log) => [{
    label: 'Profile',
    value: `<span>ID: ${log.entityId}</span>`,
  }],
};

export default membersEditIdentities;
