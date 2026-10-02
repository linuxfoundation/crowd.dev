import type { ReportDataTypeConfig } from '@/shared/modules/report-issue/config';
import Project from './type-project.vue';

const project: ReportDataTypeConfig = {
  description: (attribute: any) => `Project: ${attribute.name}`,
  display: Project,
};

export default project;
