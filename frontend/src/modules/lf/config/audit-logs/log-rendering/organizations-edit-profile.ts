import type { LogRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering/index';

function camelCaseToName(camelCase) {
  return camelCase.replace(/([A-Z])/g, ' $1')
    .replace(/^./, (str) => str.toUpperCase());
}

// Organization row written by OrganizationRepository.update.
type OrganizationProfileState = Record<string, unknown>;

const organizationsEditProfile: LogRenderingConfig<OrganizationProfileState> = {
  label: 'Organization profile updated',
  changes: ({ oldState, newState, diff }) => {
    const additions = [];
    const removals = [];
    const changes = [];

    // diff is the deep-object-diff of the two states, always an object.
    Object.keys(diff as Record<string, unknown>).forEach((key) => {
      const keyName = camelCaseToName(key);
      if (!!oldState[key] && !newState[key] && newState[key] !== undefined) {
        const display = typeof oldState[key] === 'object' ? JSON.stringify(oldState[key]) : `${oldState[key]}`;
        removals.push(`<span>${keyName}: </span> ${display}`);
      } else if (!oldState[key] && !!newState[key]) {
        const display = typeof newState[key] === 'object' ? JSON.stringify(newState[key]) : `${newState[key]}`;
        additions.push(`<span>${keyName}: </span>${display}`);
      } else if (oldState[key] !== newState[key] && newState[key] !== undefined) {
        const displayOld = typeof oldState[key] === 'object' ? JSON.stringify(oldState[key]) : `${oldState[key]}`;
        const displayNew = typeof newState[key] === 'object' ? JSON.stringify(newState[key]) : `${newState[key]}`;
        changes.push(`<span>${keyName}: </span><s>${displayOld}</s> ${displayNew}`);
      }
    });

    return {
      additions,
      removals,
      changes,
    };
  },
  description: (log) => `ID: ${log.entityId}`,
  properties: (log) => [{
    label: 'Organization',
    value: `ID: ${log.entityId}</span>`,
  }],
};

export default organizationsEditProfile;
