<template>
  <lf-multi-select-filter v-model="form" :config="(props.config as MultiSelectFilterConfig)" :options="data.options || []" />
</template>

<script setup lang="ts">
import {
  computed, watch,
} from 'vue';
import LfMultiSelectFilter from '@/shared/modules/filters/components/filterTypes/MultiSelectFilter.vue';
import type {
  MultiSelectFilterConfig, MultiSelectFilterOptionGroup,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import { extractRepoNameFromUrl } from '@/utils/string';
import { useActivityStore } from '@/modules/activity/store/pinia';
import { storeToRefs } from 'pinia';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  modelValue: string,
  config: CustomFilterConfig,
  data: any,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void, (e: 'update:data', value: any): void}>();

const form = computed({
  get: () => props.modelValue,
  set: (value: string) => emit('update:modelValue', value),
});

const data = computed({
  get: () => props.data,
  set: (value: any) => emit('update:data', value),
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
