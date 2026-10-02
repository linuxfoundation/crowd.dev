import type { ReportDataTypeConfig } from '@/shared/modules/report-issue/config';
import ProjectAffiliation from './type-project-affiliation.vue';

export const projectAffiliation: ReportDataTypeConfig = {
  description: (attribute: any) => {
    const affiliations = Object.values(attribute.affiliations).flat();

    const list: string[] = [
      `Project: ${attribute.name}`,
    ];
    if (affiliations.length > 0) {
      list.push(`Affiliation: ${affiliations[0].organizationName}`);
    }
    return list.join('\n');
  },
  display: ProjectAffiliation,
};

export default projectAffiliation;
