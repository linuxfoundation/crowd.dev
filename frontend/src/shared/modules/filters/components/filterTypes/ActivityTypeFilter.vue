<!-- Copyright (c) 2026 The Linux Foundation and each contributor. -->
<!-- SPDX-License-Identifier: MIT -->
<template>
  <lf-multi-select-filter v-model="form" :config="multiSelectConfig" :options="data.options || []" />
</template>

<script setup lang="ts">
import {
  computed, watch,
} from 'vue';
import { storeToRefs } from 'pinia';
import LfMultiSelectFilter from '@/shared/modules/filters/components/filterTypes/MultiSelectFilter.vue';
import type {
  MultiSelectFilterConfig,
  MultiSelectFilterOptionGroup,
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import type { ActivityTypeDefinition } from '@/shared/modules/activity/types/ActivityTypes';
import { useActivityTypeStore } from '@/modules/activity/store/type';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { useIntegrationStore } from '@/modules/integration/store/integration.store';
import { lfIdentities } from '@/config/identities';
import useIntegrationsHelpers from '@/config/integrations/integrations.helpers';
import octolens, { octolensMentionPlatforms } from '@/config/integrations/octolens/config';

interface FilterData { options?: MultiSelectFilterOptionGroup[] }

const props = defineProps<{
  modelValue: MultiSelectFilterValue,
  config: CustomFilterConfig<MultiSelectFilterValue, MultiSelectFilterOptions>,
  data: FilterData,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: MultiSelectFilterValue): void, (e: 'update:data', value: FilterData): void}>();
const activityTypeStore = useActivityTypeStore();
const types = computed(() => activityTypeStore.types);

const integrationStore = useIntegrationStore();
const { getActiveIntegrations } = useIntegrationsHelpers();

const activeIntegrations = computed<string[]>(() => getActiveIntegrations().map((integration) => integration.key));

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const multiSelectConfig = computed<MultiSelectFilterConfig>(() => ({ ...props.config, type: FilterConfigType.MULTISELECT }));

const form = computed({
  get: () => props.modelValue,
  set: (value: MultiSelectFilterValue) => emit('update:modelValue', value),
});

const data = computed({
  get: () => props.data,
  set: (value: FilterData) => emit('update:data', value),
});

const toOptionGroup = (platform: string, activityTypes: Record<string, ActivityTypeDefinition>): MultiSelectFilterOptionGroup => ({
  label: lfIdentities[platform]?.name ?? platform,
  options: Object.entries(activityTypes).map(([activityType, activityTypeData]) => ({
    label: `${activityTypeData?.display.short.charAt(0).toUpperCase()}${activityTypeData?.display.short.substring(1).toLowerCase()}`,
    value: `${platform}:${activityType}`,
  })),
});

const platformOptionGroup = (
  platform: string,
  activityTypes: Record<string, ActivityTypeDefinition>,
  activeIntegrationsValue: string[],
): MultiSelectFilterOptionGroup | null => {
  if (activeIntegrationsValue.includes(platform)) {
    return toOptionGroup(platform, activityTypes);
  }
  if (
    activeIntegrationsValue.includes(octolens.key)
    && octolensMentionPlatforms.includes(platform)
    && activityTypes.mention
  ) {
    return toOptionGroup(platform, { mention: activityTypes.mention });
  }
  return null;
};

watch([types, activeIntegrations], ([typesValue, activeIntegrationsValue]) => {
  const platformsOptions = Object.entries(typesValue.default ?? {})
    .map(([platform, activityTypes]) => platformOptionGroup(platform, activityTypes, activeIntegrationsValue))
    .filter((group): group is MultiSelectFilterOptionGroup => group !== null);

  const customOptions = Object.entries(typesValue.custom ?? {})
    .map(([platform, activityTypes]) => toOptionGroup(platform, activityTypes));

  data.value.options = [
    ...platformsOptions,
    ...customOptions,
  ];
}, {
  deep: true,
  immediate: true,
});

watch(() => selectedProjectGroup.value?.id, async (projectGroupId) => {
  await integrationStore.fetch(projectGroupId ? [projectGroupId] : []);
}, { immediate: true });
</script>
