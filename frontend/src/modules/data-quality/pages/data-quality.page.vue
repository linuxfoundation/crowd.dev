<template>
  <app-page-wrapper class="!pt-0">
    <div class="">
      <!-- Header -->
      <div class="sticky -top-5 bg-white z-10 pt-5 -mt-5 pb-6">
        <div class="flex justify-between items-center">
          <div>
            <app-lf-page-header text-class="text-tiny text-secondary-400 mb-1" />
            <h4 class="font-semibold">
              Data Quality Copilot
            </h4>
          </div>

          <!-- Tabs -->
          <lf-tabs v-model="tab">
            <lf-tab v-model="tab" name="member" preserve-query>
              People
            </lf-tab>
            <lf-tab v-model="tab" name="organization" preserve-query>
              Organizations
            </lf-tab>
          </lf-tabs>
        </div>
      </div>

      <!-- Content -->
      <div>
        <lf-data-quality-member v-if="tab === 'member'" />
        <lf-data-quality-organization v-else />
      </div>
    </div>
  </app-page-wrapper>
</template>

<script lang="ts" setup>
import AppPageWrapper from '@/shared/layout/page-wrapper.vue';
import AppLfPageHeader from '@/modules/lf/layout/components/lf-page-header.vue';
import { ref } from 'vue';
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import LfDataQualityMember from '@/modules/data-quality/components/data-quality-member.vue';
import LfDataQualityOrganization from '@/modules/data-quality/components/data-quality-organization.vue';

defineOptions({
  name: 'DataQualityPage',
});

const tab = ref('member');
</script>
