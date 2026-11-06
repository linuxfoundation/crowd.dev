<template>
  <div
    class="flex items-center gap-3"
  >
    <div
      v-if="changes || insertions || deletions"
      class="flex items-center"
    >
      <div
        class="flex items-center tag h-8 !rounded-l-md !rounded-r-none !bg-white"
      >
        <lf-icon name="file-pen" type="regular" :size="16" class="mr-2 text-gray-400" />
        <p
          class="text-xs text-gray-900"
        >
          {{ pluralize(changesCopy, changes || 0, true) }}
        </p>
      </div>
      <div class="bg-white h-8 rounded-r-md flex items-center gap-2 text-xs px-3 border-y border-r border-gray-200">
        <div class="text-green-600">
          +{{ insertions || 0 }}
        </div>
        <div class="text-red-600">
          -{{ deletions || 0 }}
        </div>
      </div>
    </div>
    <el-tooltip v-if="sourceId && displaySourceId" placement="top" :content="sourceId">
      <div class="text-gray-500 text-xs max-w-sm whitespace-nowrap overflow-hidden text-ellipsis">
        <span class="font-semibold">SHA:</span> {{ sourceId }}
      </div>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import pluralize from 'pluralize';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'AppActivityContentFooter',
});

withDefaults(defineProps<{
  changes?: number | null;
  changesCopy: string;
  insertions?: number | null;
  deletions?: number | null;
  sourceId?: string | null;
  displaySourceId?: boolean;
}>(), {
  changes: null,
  insertions: null,
  deletions: null,
  sourceId: null,
  displaySourceId: false,
});

</script>
