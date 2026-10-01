<template>
  <router-link
    v-if="organization"
    :to="{
      name: 'organizationView',
      params: {
        id: organization.id,
      },
      query: {
        projectGroup: selectedProjectGroup?.id,
        segmentId,
      },
    }"
    class="group hover:cursor-pointer"
  >
    <div
      class="flex items-center gap-1.5"
      :class="{
        'pl-0.5': organization.logo,
        'pl-1.5': !organization.logo,
      }"
    >
      <img
        v-if="organization.logo"
        class="w-4 h-4 border border-gray-100 rounded-[4px]"
        :src="organization.logo"
        :alt="`${organization.displayName} logo`"
      />
      <div class="flex items-center gap-1">
        <span
          class="text-gray-500 group-hover:decoration-gray-900 transition text-xs underline decoration-dashed underline-offset-4 decoration-gray-400"
        >{{ organization.displayName }}</span>
        <lf-organization-lf-member-tag
          :organization="organization"
          only-show-icon
        />
      </div>
    </div>
  </router-link>
</template>

<script setup lang="ts">
import type { Organization } from '@/modules/organization/types/Organization';
import { computed } from 'vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { useActivityStore } from '@/modules/activity/store/pinia';
import LfOrganizationLfMemberTag from '@/modules/organization/components/lf-member/organization-lf-member-tag.vue';

defineProps<{
  organization: Organization;
}>();

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const activityStore = useActivityStore();
const { filters } = storeToRefs(activityStore);

const segmentId = computed(() => {
  if (!filters.value.projects) {
    return selectedProjectGroup.value?.id;
  }

  return filters.value.projects.value[0];
});
</script>
