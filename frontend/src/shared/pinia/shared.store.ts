import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { ReportDataModalPayload } from '@/shared/modules/report-issue/config';

export const useSharedStore = defineStore('shared', () => {
  const reportDataModal = ref<ReportDataModalPayload | null>(null);

  function setReportDataModal(data: ReportDataModalPayload): void {
    reportDataModal.value = data;
  }

  return { reportDataModal, setReportDataModal };
});
