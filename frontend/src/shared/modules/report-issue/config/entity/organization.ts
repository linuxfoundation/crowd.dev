import type { ReportDataConfig } from '@/shared/modules/report-issue/config';
import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';

const person: ReportDataConfig = {
  url: (id: string) => `/organization/${id}/data-issue`,
  types: [
    ReportDataType.ORGANIZATION_DETAILS,
    ReportDataType.IDENTITY,
    ReportDataType.DOMAIN,
    ReportDataType.OTHER,
  ],
};

export default person;
