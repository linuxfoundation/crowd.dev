import type { Component } from 'vue';
import type {
  Contributor,
  ContributorAffiliation,
  ContributorIdentity,
} from '@/modules/contributor/types/Contributor';
import type { Organization, OrganizationIdentity } from '@/modules/organization/types/Organization';
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
export type ReportDataProject = Contributor['segments'][number] & {
  affiliations: Record<string, ContributorAffiliation[]>;
};

export type ReportDataAttribute =
  ContributorIdentity | OrganizationIdentity | Organization | ReportDataProject;

export interface ReportDataModalPayload {
  type?: ReportDataType;
  attribute?: ReportDataAttribute;
  contributor?: Contributor;
  organization?: Organization;
}

export interface ReportDataTypeConfig {
  // method syntax keeps per-type configs assignable with narrower attribute types
  description(
    attribute: ReportDataAttribute | undefined,
    entity: Contributor | Organization | undefined,
  ): string;
  display: Component | null;
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
