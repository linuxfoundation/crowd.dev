<template>
  <div>
    <lf-report-data-issue-modal
      v-if="reportDataModal !== null"
      model-value
      v-bind="reportDataModal"
      @update:model-value="reportDataModal = null"
    />
  </div>
</template>

<script setup lang="ts">

import { useSharedStore } from '@/shared/pinia/shared.store';
import { storeToRefs } from 'pinia';
import LfReportDataIssueModal from '@/shared/modules/report-issue/component/report-data-issue-modal.vue';

const sharedStore = useSharedStore();
const { reportDataModal } = storeToRefs(sharedStore);
</script>

<script lang="ts">
export default {
  name: 'LfGlobals',
};
</script>
