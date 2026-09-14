<template>
  <div>
    <div class="sticky top-18 bg-white z-10 -mt-1">
      <div class="flex mb-6 justify-between items-center">
        <div class="flex items-center gap-4">
          <lf-data-quality-project-dropdown v-model="projectGroup" />
          <lf-data-quality-type-dropdown
            v-model="tab"
            :config="[]"
            :suggestion-types="['fake-suggestions']"
          />
        </div>
        <p class="text-small text-gray-500 italic">
          <span v-if="tab === 'merge-suggestions'">Sorted by confidence level</span>
          <span v-else>Sorted by number of activities</span>
        </p>
      </div>
      <div class="border-b border-gray-200 w-full mb-1" />
    </div>
    <div>
      <lf-data-quality-organization-merge-suggestions
        v-if="tab === 'merge-suggestions'"
        :project-group="projectGroup"
      />
      <lf-data-quality-fake-organization-suggestions
        v-else-if="tab === 'fake-suggestions'"
        :project-group="projectGroup"
      />
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref } from 'vue';
import LfDataQualityOrganizationMergeSuggestions
  from '@/modules/data-quality/components/organization/data-quality-organization-merge-suggestions.vue';
import LfDataQualityFakeOrganizationSuggestions
  from '@/modules/data-quality/components/organization/data-quality-fake-organization-suggestions.vue';
import LfDataQualityTypeDropdown from '@/modules/data-quality/components/shared/data-quality-type-dropdown.vue';
import LfDataQualityProjectDropdown from '@/modules/data-quality/components/shared/data-quality-project-dropdown.vue';
import { useRoute } from 'vue-router';

const route = useRoute();

const tab = ref('merge-suggestions');
const projectGroup = ref(route.query.projectGroup as string);
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityOrganization',
};
</script>
