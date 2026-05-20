<template>
  <div>
    <div class="flex items-center gap-1">
      <el-popover trigger="hover" placement="top" popper-class="!w-72">
        <template #reference>
          <div class="flex items-center gap-1">
            <div class="text-gray-600 text-2xs flex items-center leading-5 font-medium">
              <lf-icon name="calendar" class="!text-gray-600 mr-1 h-4 flex items-center" />
              {{ events.length }} {{ events.length === 1 ? 'event' : 'events' }}
            </div>
          </div>
        </template>

        <div class="max-h-44 overflow-auto -my-1 px-1">
          <p class="text-gray-400 text-sm font-semibold mb-4">
            Sched events
          </p>
          <article
            v-for="event in events"
            :key="event.eventId"
            class="flex flex-col mb-4 last:mb-0"
          >
            <span class="text-gray-900 text-sm font-medium max-w-3xs truncate">
              {{ event.eventName || event.subdomain }}
            </span>
            <span class="text-gray-500 text-xs max-w-3xs truncate">
              {{ event.subdomain }}.sched.com
            </span>
          </article>
        </div>
      </el-popover>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const events = computed<{ eventId: string; eventName: string; subdomain: string }[]>(
  () => props.integration?.settings?.events ?? [],
);
</script>

<script lang="ts">
export default {
  name: 'LfSchedParams',
};
</script>
