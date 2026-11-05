<template>
  <p v-if="source" class="text-tiny text-gray-400">
    Source: <span v-html="$sanitize(source)" />
  </p>
</template>

<script setup lang="ts">
import { getAttributeSourceName } from '@/shared/helpers/attribute.helpers';
import { computed } from 'vue';
import type { AttributeValue } from '@/modules/contributor/types/Contributor';

const props = defineProps<{
  values: Record<string, AttributeValue>,
}>();

const source = computed(() => getAttributeSourceName(props.values));
</script>

<script lang="ts">
export default {
  name: 'LfContributorAttributeSource',
};
</script>
