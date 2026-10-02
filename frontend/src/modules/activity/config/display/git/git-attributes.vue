<template>
  <div
    v-if="
      activity.attributes.lines
        || activity.attributes.insertions
        || activity.attributes.deletions
    "
    class="flex items-center text-sm text-gray-600 gap-2"
  >
    <lf-icon
      name="file-pen"
      :size="14"
      class="text-gray-600"
    />
    <p class="text-sm text-gray-600" />
    <p class="text-2xs text-gray-600">
      {{ pluralize("line", activity.attributes.lines || 0, true) }}
    </p>
    <div class="flex items-center gap-1">
      <div class="text-green-600 text-2xs">
        +{{ activity.attributes.insertions || 0 }}
      </div>
      <div class="text-red-600 text-2xs">
        -{{ activity.attributes.deletions || 0 }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Activity } from '@/shared/modules/activity/types/Activity';
import pluralize from 'pluralize';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineProps<{
  activity: Activity;
}>();
</script>
