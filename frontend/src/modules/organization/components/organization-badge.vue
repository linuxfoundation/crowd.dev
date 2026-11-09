<template>
  <div
    v-if="isTeam"
    class="inline-flex items-center gap-1 min-w-fit"
  >
    <el-tooltip
      placement="top"
      content="Your own organization"
    >
      <div class="px-2 py-0.5 flex items-center text-2xs font-medium bg-gray-200 text-gray-600 rounded-md gap-1">
        <lf-icon name="people-group" :size="12" />Team
      </div>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Organization } from '@/modules/organization/types/Organization';

defineOptions({ name: 'AppOrganizationBadge' });

const props = defineProps<{
  organization: Organization;
}>();

const isTeam = computed(() => props.organization.isTeamOrganization);
</script>
