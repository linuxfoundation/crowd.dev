import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';

const tooManyEmails: DataIssueTypeConfig = {
  label: 'More than 5 verified emails',
  badgeType: 'warning',
  badgeText: () => 'More than 5 verified emails',
  description: () => null,
};

export default tooManyEmails;
