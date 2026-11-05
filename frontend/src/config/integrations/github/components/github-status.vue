<template>
  <div
    v-if="props.integration?.status === 'mapping'"
    class="flex items-center gap-1"
  >
    <lf-svg name="git-repository" class="w-4 h-4" />
    Mapping repositories...
  </div>
</template>

<script setup lang="ts">
import type { Integration } from '@/modules/admin/modules/integration/types/Integration';
import LfSvg from '@/shared/svg/svg.vue';

const props = defineProps<{
  integration: Integration<unknown>;
}>();
</script>

<script lang="ts">
export default {
  name: 'LfGithubStatus',
};
</script>
