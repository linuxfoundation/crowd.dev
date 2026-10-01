import type { Contributor } from '@/modules/contributor/types/Contributor';
import type { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import type { Organization } from '@/modules/organization/types/Organization';

export default {
  /** Report Data Modal * */
  setReportDataModal(data: {
    type?: ReportDataType,
    attribute?: any,
    contributor?: Contributor,
    organization?: Organization,
  }) {
    this.reportDataModal = data;
  },
};
