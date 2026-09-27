import type { LogRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering/index';
import { lfIdentities } from '@/config/identities';

type OrganizationIdentitiesState = Record<string, { value: string; type: string; verified: boolean }[]>;

const organizationsEditIdentities: LogRenderingConfig<OrganizationIdentitiesState> = {
  label: 'Organization identities updated',
  changes: ({ oldState, newState }) => {
    const additions: any[] = [];
    const removals: any[] = [];

    // Helper function to process states
    const keys = new Set([...Object.keys(oldState), ...Object.keys(newState)]);
    keys.forEach((key) => {
      const oldValues = new Set(oldState[key] || []);
      const newValues = new Set(newState[key] || []);

      newValues.forEach((value) => {
        if (!oldValues.has(value)) {
          additions.push({ platform: key, name: value });
        }
      });

      oldValues.forEach((value) => {
        if (!newValues.has(value)) {
          removals.push({ platform: key, name: value });
        }
      });
    });

    return {
      additions: (additions || []).map((p) => `${lfIdentities[p.platform]?.name || p.platform}: ${p.name}`),
      removals: (removals || []).map((p) => `${lfIdentities[p.platform]?.name || p.platform}: ${p.name}`),
      changes: [],
    };
  },
  description: (log) => `ID: ${log.entityId}`,
  properties: (log) => [{
    label: 'Organization',
    value: `ID: ${log.entityId}</span>`,
  }],
};

export default organizationsEditIdentities;
