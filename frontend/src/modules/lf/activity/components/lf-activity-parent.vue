<template>
  <div class="flex items-center gap-3 mt-3">
    <span class="block w-1 min-h-5 h-full bg-gray-200" />
    <div class="text-gray-600 text-xs">
      <span>{{ toSentenceCase(parent.display.short) }}</span>
      <span v-if="parent.member"> from <router-link
        :to="{
          name: 'memberView',
          params: {
            id: parent.memberId,
          },
          query: { projectGroup: selectedProjectGroup?.id },
        }"
      >
        <span class="font-medium text-gray-900">{{ parent.member.displayName }}</span>
      </router-link></span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { toSentenceCase } from '@/utils/string';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import type { Activity } from '@/shared/modules/activity/types/Activity';

defineOptions({ name: 'AppLfActivityParent' });

defineProps<{
  parent: Activity;
}>();

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
</script>
