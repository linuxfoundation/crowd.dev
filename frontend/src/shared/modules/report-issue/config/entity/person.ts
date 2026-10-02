import type { ReportDataConfig } from '@/shared/modules/report-issue/config';
import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';

const person: ReportDataConfig = {
  url: (id: string) => `/member/${id}/data-issue`,
  types: [
    ReportDataType.PROFILE_DETAILS,
    ReportDataType.PROJECT,
    ReportDataType.PROJECT_AFFILIATION,
    ReportDataType.WORK_EXPERIENCE,
    ReportDataType.IDENTITY,
    ReportDataType.OTHER,
  ],
};

export default person;
