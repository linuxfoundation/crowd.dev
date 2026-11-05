<template>
  <p v-if="source" class="text-tiny text-gray-400">
    Source: <span v-html="$sanitize(source)" />
  </p>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { isEqual } from 'lodash';
import { lfIdentities } from '@/config/identities';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';
import type { OrganizationAttribute } from '@/modules/organization/types/Organization';

const props = defineProps<{
  values: OrganizationAttribute,
}>();

const { getPlatformsLabel } = useIdentitiesHelpers();

const source = computed(() => {
  if (!props.values) {
    return null;
  }
  const defaultValue = props.values.default;
  let sources = Object.keys(props.values)
    .filter((key) => !['default'].includes(key) && (props.values[key] as unknown[]).some((value) => isEqual(value, defaultValue)));
  if (!props.values.default) {
    sources = Object.keys(props.values);
  }

  if (sources.length === 0) {
    return null;
  }

  // Sort that integrations are first, then others like enrichment and last is custom
  const prioritySortedSources = sources.sort((a, b) => {
    const aConfig = !!lfIdentities[a]?.name;
    const bConfig = !!lfIdentities[b]?.name;

    if (aConfig && !bConfig) return -1;
    if (!aConfig && bConfig) return 1;

    return 0;
  });
  const selectedSource = prioritySortedSources[0];
  return getPlatformsLabel([selectedSource]);
});
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationAttributeSource',
};
</script>
