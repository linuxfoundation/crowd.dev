<template>
  <el-tabs v-model="activeTab" class="integration-tabs">
    <el-tab-pane v-for="tab in tabs" :key="tab.key" :label="tab.label + ` (${tab.count})`" :name="tab.key">
      <template #label>
        <div class="flex items-center gap-1.5">
          <lf-icon :name="tab.icon" :size="20" class="text-gray-600" />
          <span>{{ tab.label }} ({{ tab.count }})</span>
        </div>
      </template>
    </el-tab-pane>
  </el-tabs>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { IntegrationTabs } from '../../types/overview.types';

const emit = defineEmits<{(e: 'update:modelValue', value: string): void;
}>();

const props = defineProps<{
  tabs: IntegrationTabs[];
  modelValue: string;
}>();

const activeTab = computed<string>({
  get() {
    return props.modelValue;
  },
  set(value: string) {
    emit('update:modelValue', value);
  },
});
</script>

<script lang="ts">
export default {
  name: 'AppLfOverviewIntegrationTabs',
};
</script>

<style scoped>
.integration-tabs :deep(.el-tabs__nav-wrap::after) {
  height: 1px;
}

.integration-tabs :deep(.el-tabs__active-bar) {
  height: 1px;
}
</style>
