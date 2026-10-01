import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';

const workExperienceMissingInfo: DataIssueTypeConfig = {
  label: 'Work experience(s) with missing information',
  badgeType: 'warning',
  badgeText: () => 'Work experience(s) with missing information',
  description: () => null,
};

export default workExperienceMissingInfo;
