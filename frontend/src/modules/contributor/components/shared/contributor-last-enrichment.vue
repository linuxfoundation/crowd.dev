<template>
  <div v-if="props.contributor.lastEnrichedAt" class="flex items-center mr-4">
    <p class="text-tiny text-gray-500 mr-2 text-right">
      Last enrichment on {{ dateHelper(props.contributor.lastEnrichedAt).format('MMM DD, YYYY') }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { dateHelper } from '@/shared/date-helper/date-helper';
import type { Contributor } from '@/modules/contributor/types/Contributor';

const props = defineProps<{
  contributor: Contributor,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfContributorLastEnrichment',
};
</script>
