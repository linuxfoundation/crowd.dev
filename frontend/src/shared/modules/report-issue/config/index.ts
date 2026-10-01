import type { ReportDataEntity } from '@/shared/modules/report-issue/constants/report-data-entity.enum';

import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import person from './entity/person';
import organization from './entity/organization';
import profileDetails from './type/profile-details/profile-details';
import projectAffiliation from './type/project-affiliation/project-affiliation';
import project from './type/project/project';
import workExperience from './type/work-experience/work-experience';
import identity from './type/identity/identity';
import organizationDetails from './type/organization-details/organization-details';
import domain from './type/domain/domain';
import other from './type/other/other';

export interface ReportDataConfig {
  url: (id: string) => string;
  types: ReportDataType[];
}
export interface ReportDataTypeConfig {
  description: (attribute: any, entity: any) => string;
  display: any | null;
}

export const reportDataConfig: Record<ReportDataEntity, ReportDataConfig> = {
  person,
  organization,
};

export const reportDataTypeDisplay: Record<ReportDataType, ReportDataTypeConfig> = {
  [ReportDataType.PROFILE_DETAILS]: profileDetails,
  [ReportDataType.PROJECT]: project,
  [ReportDataType.PROJECT_AFFILIATION]: projectAffiliation,
  [ReportDataType.WORK_EXPERIENCE]: workExperience,
  [ReportDataType.IDENTITY]: identity,
  [ReportDataType.ORGANIZATION_DETAILS]: organizationDetails,
  [ReportDataType.DOMAIN]: domain,
  [ReportDataType.OTHER]: other,
};
