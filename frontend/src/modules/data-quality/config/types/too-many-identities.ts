import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';

const tooManyIdentities: DataIssueTypeConfig = {
  label: 'More than 30 identities',
  badgeType: 'danger',
  badgeText: () => 'More than 30 identities',
  description: () => null,
};

export default tooManyIdentities;
