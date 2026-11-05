<template>
  <div>
    <div class="flex items-center gap-1">
      <el-popover trigger="hover" placement="top" popper-class="!w-72">
        <template #reference>
          <div
            class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
          >
            <lf-icon name="comments-question-check" :size="16" class="text-gray-600 mr-1 flex items-center" />

            {{ pluralize('group', groups.length, true) }}
          </div>
        </template>

        <p class="text-gray-400 text-sm font-semibold mb-4">
          Groups.io groups
        </p>
        <div class="max-h-44 overflow-auto -my-1 px-1">
          <article
            v-for="group of groups"
            :key="group.id"
            class="flex items-center flex-nowrap mb-4 last:mb-0"
          >
            <lf-icon name="comments-question-check" :size="16" class="mr-1 flex items-center" />
            <span class="text-gray-900 text-sm max-w-3xs truncate">{{ group.name }}</span>
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

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const groups = computed<{ id: string; name: string }[]>(() => props.integration.settings.groups);
</script>

<script lang="ts">
export default {
  name: 'LfGroupsioParams',
};
</script>
