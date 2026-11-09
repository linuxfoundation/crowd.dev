<template>
  <div>
    <div class="sticky top-18 bg-white z-10 -mt-1">
      <div class="flex mb-6 justify-between items-center">
        <div class="flex items-center gap-4">
          <lf-data-quality-project-dropdown v-model="projectGroup" />
          <lf-data-quality-type-dropdown
            v-model="tab"
            :config="memberDataIssueTypeMenu"
          />
        </div>
        <p class="text-small text-gray-500 italic">
          <span v-if="tab === 'merge-suggestions' || tab === 'bot-suggestions'">Sorted by confidence level</span>
          <span v-else>Sorted by number of activities</span>
        </p>
      </div>
      <div class="border-b border-gray-200 w-full mb-1" />
    </div>

    <div>
      <lf-data-quality-member-merge-suggestions
        v-if="tab === 'merge-suggestions'"
        :project-group="projectGroup"
      />
      <lf-data-quality-member-bot-suggestions
        v-else-if="tab === 'bot-suggestions'"
        :project-group="projectGroup"
      />
      <lf-data-quality-member-issues
        v-else
        :type="tab as DataIssueType"
        :project-group="projectGroup"
      />
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref } from 'vue';
import type { DataIssueType } from '@/modules/data-quality/types/DataIssueType';
import LfDataQualityMemberMergeSuggestions
  from '@/modules/data-quality/components/member/data-quality-member-merge-suggestions.vue';
import LfDataQualityMemberIssues from '@/modules/data-quality/components/member/data-quality-member-issues.vue';
import LfDataQualityTypeDropdown from '@/modules/data-quality/components/shared/data-quality-type-dropdown.vue';
import LfDataQualityProjectDropdown from '@/modules/data-quality/components/shared/data-quality-project-dropdown.vue';
import { useRoute } from 'vue-router';
import { memberDataIssueTypeMenu } from '@/modules/data-quality/config/data-issue-types';
import LfDataQualityMemberBotSuggestions
  from '@/modules/data-quality/components/member/data-quality-member-bot-suggestions.vue';

defineOptions({
  name: 'LfDataQualityMember',
});

const route = useRoute();

const tab = ref('merge-suggestions');
const projectGroup = ref(route.query.projectGroup as string);
</script>
