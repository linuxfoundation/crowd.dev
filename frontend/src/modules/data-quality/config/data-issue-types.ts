import { DataIssueType } from '@/modules/data-quality/types/DataIssueType';
import noWorkExperience from '@/modules/data-quality/config/types/no-work-experience';
import workExperienceMissingInfo from '@/modules/data-quality/config/types/work-experience-missing-info';
import workExperienceMissingPeriod from '@/modules/data-quality/config/types/work-experience-missing-period';
import tooManyIdentitiesPerPlatform from '@/modules/data-quality/config/types/too-many-identities-per-platform';
import tooManyEmails from '@/modules/data-quality/config/types/too-many-emails';
import tooManyIdentities from '@/modules/data-quality/config/types/too-many-identities';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import type { Organization } from '@/modules/organization/types/Organization';
import conflictingWorkExperience from '@/modules/data-quality/config/types/conflicting-work-experience';

export interface DataIssueTypeConfig{
  label: string;
  badgeType: string;
  badgeText: (entity: Contributor | Organization) => string;
  description: (entity: Contributor | Organization) => string;
}

export interface DataIssueTypeMenu{
  label?: string;
  types: DataIssueType[]
}

export const dataIssueTypes: Record<DataIssueType, DataIssueTypeConfig> = {
  [DataIssueType.TOO_MANY_IDENTITIES]: tooManyIdentities,
  [DataIssueType.TOO_MANY_IDENTITIES_PER_PLATFORM]: tooManyIdentitiesPerPlatform,
  [DataIssueType.TOO_MANY_EMAILS]: tooManyEmails,
  [DataIssueType.NO_WORK_EXPERIENCE]: noWorkExperience,
  [DataIssueType.WORK_EXPERIENCE_MISSING_INFO]: workExperienceMissingInfo,
  [DataIssueType.WORK_EXPERIENCE_MISSING_PERIOD]: workExperienceMissingPeriod,
  [DataIssueType.CONFLICTING_WORK_EXPERIENCE]: conflictingWorkExperience,
};

export const memberDataIssueTypeMenu: DataIssueTypeMenu[] = [
  {
    label: 'Identities',
    types: [
      DataIssueType.TOO_MANY_IDENTITIES,
      DataIssueType.TOO_MANY_IDENTITIES_PER_PLATFORM,
      DataIssueType.TOO_MANY_EMAILS,
    ],
  },
  {
    label: 'Work history',
    types: [
      DataIssueType.WORK_EXPERIENCE_MISSING_INFO,
      DataIssueType.NO_WORK_EXPERIENCE,
      DataIssueType.WORK_EXPERIENCE_MISSING_PERIOD,
      DataIssueType.CONFLICTING_WORK_EXPERIENCE,
    ],
  },
];
