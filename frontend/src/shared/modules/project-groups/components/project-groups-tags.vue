<template>
  <div v-if="projectGroups.length">
    <el-popover
      trigger="hover"
      placement="top"
      popper-class="!w-[260px] !max-w-[260px] !max-h-[400px] overflow-auto"
      :disabled="projectGroups.length === 1"
    >
      <template #reference>
        <lf-tag type="secondary" size="small">
          {{ projectGroups.length > 1
            ? pluralize('project group', projectGroups.length, true)
            : projectGroups[0].name }}
        </lf-tag>
      </template>

      <div>
        <div class="mb-2 text-gray-400 text-2xs">
          Project groups
        </div>
        <div class="flex flex-wrap items-center gap-1">
          <div v-for="projectGroup in projectGroups" :key="projectGroup.id">
            <lf-tag type="secondary" size="small" bg-color="white">
              {{ projectGroup.name }}
            </lf-tag>
          </div>
        </div>
      </div>
    </el-popover>
  </div>
</template>

<script setup lang="ts">
import { getProjectGroupsThroughSegments } from '@/utils/segments';
import { computed } from 'vue';
import pluralize from 'pluralize';
import LfTag from '@/ui-kit/tag/Tag.vue';

defineOptions({
  name: 'LfProjectGroupsTags',
});

const props = defineProps<{
  segments: string[];
}>();

const projectGroups = computed(() => getProjectGroupsThroughSegments(props.segments));
</script>
