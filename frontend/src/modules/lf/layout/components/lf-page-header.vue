<template>
  <div
    class="break-words"
    :class="textClass"
  >
    {{ selectedProjectGroup?.name }}
  </div>
</template>

<script setup lang="ts">
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

defineOptions({ name: 'AppLfPageHeader' });

withDefaults(defineProps<{
  textClass?: string | null;
}>(), {
  textClass: null,
});

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
</script>
