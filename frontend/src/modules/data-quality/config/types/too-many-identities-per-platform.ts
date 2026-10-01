import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';

const tooManyIdentitiesPerPlatform: DataIssueTypeConfig = {
  label: 'More than 1 verified identity per platform',
  badgeType: 'warning',
  badgeText: () => 'More than 1 verified identity per platform',
  description: () => null,
};

export default tooManyIdentitiesPerPlatform;
