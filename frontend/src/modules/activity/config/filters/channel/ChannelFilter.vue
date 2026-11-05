<template>
  <lf-multi-select-filter v-model="form" :config="multiSelectConfig" :options="data.options || []" />
</template>

<script setup lang="ts">
import {
  computed, watch,
} from 'vue';
import LfMultiSelectFilter from '@/shared/modules/filters/components/filterTypes/MultiSelectFilter.vue';
import type {
  MultiSelectFilterConfig, MultiSelectFilterOptionGroup, MultiSelectFilterOptions, MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import { extractRepoNameFromUrl } from '@/utils/string';
import { useActivityStore } from '@/modules/activity/store/pinia';
import { storeToRefs } from 'pinia';
import { lfIdentities } from '@/config/identities';

interface FilterData { options?: MultiSelectFilterOptionGroup[] }

const props = defineProps<{
  modelValue: MultiSelectFilterValue,
  config: CustomFilterConfig<MultiSelectFilterValue, MultiSelectFilterOptions>,
  data: FilterData,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: MultiSelectFilterValue): void, (e: 'update:data', value: FilterData): void}>();

const multiSelectConfig = computed<MultiSelectFilterConfig>(() => ({ ...props.config, type: FilterConfigType.MULTISELECT }));

const form = computed({
  get: () => props.modelValue,
  set: (value: MultiSelectFilterValue) => emit('update:modelValue', value),
});

const data = computed({
  get: () => props.data,
  set: (value: FilterData) => emit('update:data', value),
});

const activityStore = useActivityStore();
const { activityChannels } = storeToRefs(activityStore);

watch(() => activityChannels.value, () => {
  data.value.options = Object.entries(activityChannels.value).map(([platform, channels]): MultiSelectFilterOptionGroup => ({
    label: lfIdentities[platform]?.name ?? platform,
    options: channels.map((channel) => ({
      value: channel,
      label: platform === 'github' ? extractRepoNameFromUrl(channel) : channel,
    })),
  }));
}, { immediate: true });
</script>
