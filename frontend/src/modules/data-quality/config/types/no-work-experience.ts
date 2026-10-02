import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';

const noWorkExperience: DataIssueTypeConfig = {
  label: 'Missing work experiences',
  badgeType: 'danger',
  badgeText: () => 'Missing work experiences',
  description: () => null,
};

export default noWorkExperience;
