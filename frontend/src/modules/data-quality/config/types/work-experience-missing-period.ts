import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';

const workExperienceMissingPeriod: DataIssueTypeConfig = {
  label: 'Work experience(s) without period',
  badgeType: 'danger',
  badgeText: () => 'Work experience(s) without period',
  description: () => null,
};

export default workExperienceMissingPeriod;
