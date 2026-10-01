import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';

const conflictingWorkExperience: DataIssueTypeConfig = {
  label: 'Work experiences with overlapping periods',
  badgeType: 'warning',
  badgeText: () => 'Work experiences with overlapping periods',
  description: () => null,
};

export default conflictingWorkExperience;
