<template>
  <div>
    <div class="flex items-center gap-1">
      <template v-if="channels.length === 0">
        <div
          class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
        >
          <lf-icon name="messages" :size="16" class="!text-gray-600 mr-1 h-4 flex items-center" />
          All channels
        </div>
      </template>
      <el-popover v-else trigger="hover" placement="top" popper-class="!w-72">
        <template #reference>
          <div
            class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
          >
            <lf-icon name="messages" :size="16" class="!text-gray-600 mr-1 h-4 flex items-center" />

            {{ pluralize("channel", channels.length, true) }}
          </div>
        </template>

        <p class="text-gray-400 text-sm font-semibold mb-4">
          Slack channels
        </p>
        <div class="max-h-44 overflow-auto -my-1 px-1">
          <article
            v-for="channel of channels"
            :key="channel.id"
            class="flex items-center flex-nowrap mb-4 last:mb-0"
          >
            <lf-icon name="messages" :size="16" class="mr-1 h-4 flex items-center" />

            <span class="text-gray-900 text-sm max-w-3xs truncate">{{
              channel.name
            }}</span>
          </article>
        </div>
      </el-popover>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import pluralize from 'pluralize';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'LfSlackParams',
});

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const channels = computed<{ name: string; id: string }[]>(
  () => props.integration.settings.channels,
);
</script>
