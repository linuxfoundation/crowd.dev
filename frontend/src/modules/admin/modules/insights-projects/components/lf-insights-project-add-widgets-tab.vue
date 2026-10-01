<template>
  <div class="flex flex-col relative">
    <div v-if="isLoading" class="absolute left-0 top-0 w-full h-full bg-white opacity-50 z-20" />
    <div v-for="group in WIDGETS_GROUPS" :key="group.name" class="flex flex-col">
      <div class="text-2xs font-semibold bg-gray-50 border-t border-b border-gray-200 mx-[-24px] px-6 py-4">
        {{ group.name }}
      </div>
      <div
        v-for="(widget, index) in group.widgets"
        :key="widget.name"
        class="flex justify-between py-4"
        :class="{ 'border-b border-gray-200': group.widgets.length - 1 !== index }"
      >
        <span class="text-sm">
          {{ widget.name }}
        </span>
        <lf-switch v-model="cForm.widgets[widget.key].enabled" size="small" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { reactive } from 'vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import type { InsightsProjectAddFormModel } from '../models/insights-project-add-form.model';
import { WIDGETS_GROUPS } from '../widgets';

const props = defineProps<{
    form: InsightsProjectAddFormModel;
    isLoading: boolean;
}>();

const cForm = reactive(props.form);

</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectAddWidgetsTab',
};
</script>
