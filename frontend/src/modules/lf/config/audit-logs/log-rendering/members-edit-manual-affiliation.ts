import type { LogChanges, LogRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering/index';
import { OrganizationService } from '@/modules/organization/organization-service';
import { LfService } from '@/modules/lf/segments/lf-segments-service';
import { dateHelper } from '@/shared/date-helper/date-helper';

interface MemberAffiliationStateRow {
  organizationId: string | null;
  segmentId: string;
  dateStart?: string | null;
  dateEnd?: string | null;
}

const formatDateRange = (dateStart?: string | null, dateEnd?: string | null) => {
  // eslint-disable-next-line no-nested-ternary
  const dateStartFormat = dateStart
    ? dateHelper(dateStart).utc().format('MMMM YYYY')
    : 'Unknown';
  // eslint-disable-next-line no-nested-ternary
  const dateEndFormat = dateEnd
    ? dateHelper(dateEnd).utc().format('MMMM YYYY')
    : (dateStart ? 'Present' : 'Unknown');
  return `${dateStartFormat} -> ${dateEndFormat}`;
};

const membersEditManualAffiliation: LogRenderingConfig<MemberAffiliationStateRow[]> = {
  label: 'Profile affiliation updated',
  changes: async (log) => {
    const changes: LogChanges = {
      removals: [],
      additions: [],
      changes: [],
    };

    const oldStateMap = new Map(log.oldState.map((org) => [org.organizationId, org]));
    const newStateMap = new Map(log.newState.map((org) => [org.organizationId, org]));

    const orgIds = [
      ...new Set([
        ...log.oldState.map((org) => org.organizationId),
        ...log.newState.map((org) => org.organizationId),
      ]),
    ];

    const segmentIds = [
      ...new Set([
        ...log.oldState.map((org) => org.segmentId),
        ...log.newState.map((org) => org.segmentId),
      ]),
    ];

    const orgs: { id: string; displayName: string }[] = await OrganizationService.listByIds(orgIds);
    const segments: { id: string; name: string }[] = await LfService.listSegmentsByIds(segmentIds);

    const orgById = orgs.reduce<Record<string, string>>((obj, org) => ({
      ...obj,
      [org.id]: org.displayName,
    }), {});

    const segmentById = segments.reduce<Record<string, string>>((obj, org) => ({
      ...obj,
      [org.id]: org.name,
    }), {});

    // Check for removals and modifications
    log.oldState.forEach((org) => {
      if (!newStateMap.has(org.organizationId)) {
        const {
          organizationId, dateStart, dateEnd, segmentId,
        } = org;
        changes.removals.push(
          `<span>${organizationId ? (orgById[organizationId]) : 'Individual'}:</span> ${segmentId ? segmentById[segmentId] : 'None'}
          <br> (${formatDateRange(dateStart, dateEnd)})`,
        );
      } else {
        const newOrg = newStateMap.get(org.organizationId)!;
        if (org.dateStart !== newOrg.dateStart || org.dateEnd !== newOrg.dateEnd || org.segmentId !== newOrg.segmentId) {
          changes.changes.push(
            `<span>${org.organizationId ? (orgById[org.organizationId]) : 'Individual'} </span>: 
            <br><s>${org.segmentId ? segmentById[org.segmentId] : 'None'} (${formatDateRange(org.dateStart, org.dateEnd)})</s>
            <br>${newOrg.segmentId ? segmentById[newOrg.segmentId] : 'None'} (${formatDateRange(newOrg.dateStart, newOrg.dateEnd)})`,
          );
        }
      }
    });

    // Check for additions
    log.newState.forEach((org) => {
      if (!oldStateMap.has(org.organizationId)) {
        const {
          organizationId, dateStart, dateEnd, segmentId,
        } = org;
        changes.additions.push(
          `<span>${organizationId ? (orgById[organizationId]) : 'Individual'}:</span> ${segmentId ? segmentById[segmentId] : 'None'}
          <br> (${formatDateRange(dateStart, dateEnd)})`,
        );
      }
    });

    return changes;
  },
  description: (log) => `ID: ${log.entityId}`,
  properties: (log) => [{
    label: 'Profile',
    value: `<span>ID: ${log.entityId}</span>`,
  }],
};

export default membersEditManualAffiliation;
