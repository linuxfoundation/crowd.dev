<template>
  <lf-multi-select-filter v-model="form" :config="(props.config as MultiSelectFilterConfig)" :options="data.options || []" />
</template>

<script setup lang="ts">
import {
  computed, onMounted, watch,
} from 'vue';
import { storeToRefs } from 'pinia';
import { useStore } from 'vuex';
import LfMultiSelectFilter from '@/shared/modules/filters/components/filterTypes/MultiSelectFilter.vue';
import {
  MultiSelectFilterConfig,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import { useActivityTypeStore } from '@/modules/activity/store/type';
import { getSegmentsFromProjectGroup } from '@/utils/segments';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import useIntegrationsHelpers from '@/config/integrations/integrations.helpers';
import { lfIntegrations } from '@/config/integrations';

const props = defineProps<{
  modelValue: string,
  config: CustomFilterConfig,
  data: any,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void, (e: 'update:data', value: any): void}>();
const activityTypeStore = useActivityTypeStore();
const { types } = storeToRefs(activityTypeStore);

const store = useStore();
const { getActiveIntegrations } = useIntegrationsHelpers();

const activeIntegrations = computed<string[]>(() => getActiveIntegrations().map((integration) => integration.key));

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const form = computed({
  get: () => props.modelValue,
  set: (value: string) => emit('update:modelValue', value),
});

const data = computed({
  get: () => props.data,
  set: (value: any) => emit('update:data', value),
});

watch([types, activeIntegrations], ([typesValue, activeIntegrationsValue]) => {
  const platformsOptions = Object.entries(typesValue.default)
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    .filter(([platform, _]) => activeIntegrationsValue.includes(platform))
    .map(([platform, activityTypes]: [string, any]) => ({
      label: lfIntegrations()[platform]?.name ?? platform,
      options: Object.entries(activityTypes).map(([activityType, activityTypeData]) => ({
        label: `${activityTypeData.display.short.charAt(0).toUpperCase()}${activityTypeData.display.short.substring(1).toLowerCase()}`,
        value: `${platform}:${activityType}`,
      })),
    }));

  const customOptions = Object.entries(typesValue.custom)
    .map(([platform, activityTypes]: [string, any]) => ({
      label: lfIntegrations()[platform]?.name ?? platform,
      options: Object.entries(activityTypes).map(([activityType, activityTypeData]) => ({
        label: `${activityTypeData.display.short.charAt(0).toUpperCase()}${activityTypeData.display.short.substring(1).toLowerCase()}`,
        value: `${platform}:${activityType}`,
      })),
    }));

  data.value.options = [
    ...platformsOptions,
    ...customOptions,
  ];
}, {
  deep: true,
});

onMounted(async () => {
  await store.dispatch('integration/doFetch', getSegmentsFromProjectGroup(selectedProjectGroup.value));
});
</script>
