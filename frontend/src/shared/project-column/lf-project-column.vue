<template>
  <div v-if="displayCount > 0" class="flex flex-wrap gap-2">
    <el-popover
      v-if="projects.length"
      placement="top"
      :width="250"
      trigger="hover"
    >
      <template #reference>
        <app-lf-project-count :title="props.title" :icon="props.icon" :count="displayCount" />
      </template>
      <template #default>
        <div class="flex flex-wrap gap-1 overflow-hidden">
          <div
            v-for="project of projects"
            :key="project.id"
            class="truncate"
          >
            <div class="badge--border !block badge--gray-light h-6 text-xs" @click.prevent>
              {{ project.name }}
            </div>
          </div>
        </div>
      </template>
    </el-popover>
    <app-lf-project-count
      v-else
      :title="props.title"
      :icon="props.icon"
      :count="displayCount"
    />
  </div>
  <span v-else class="text-gray-500 text-sm">No {{ props.title }}</span>
</template>

<script lang="ts" setup>
import { computed } from 'vue';
import AppLfProjectCount from './lf-project-count.vue';

const props = withDefaults(defineProps<{
  projects?: {
    id: string;
    name: string;
  }[];
  projectCount?: number;
  icon?: string;
  title?: string;
}>(), {
  projects: () => [],
  projectCount: undefined,
  title: () => 'Projects',
  icon: () => 'layer-group',
});

const displayCount = computed(() => props.projectCount ?? props.projects?.length ?? 0);
</script>

<script lang="ts">
export default {
  name: 'AppLfProjectColumn',
};
</script>
