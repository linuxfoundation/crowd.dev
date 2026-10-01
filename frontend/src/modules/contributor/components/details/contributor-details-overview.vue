<template>
  <lf-contributor-details-community :contributor="props.contributor" class="mb-6" />
  <lf-contributor-details-projects :contributor="props.contributor" class="mb-10" />
  <lf-contributor-details-attributes :contributor="props.contributor" />
</template>

<script setup lang="ts">
import LfContributorDetailsCommunity
  from '@/modules/contributor/components/details/overview/contributor-details-community.vue';
import LfContributorDetailsProjects
  from '@/modules/contributor/components/details/overview/contributor-details-projects.vue';
import LfContributorDetailsAttributes
  from '@/modules/contributor/components/details/overview/contributor-details-attributes.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';

const props = defineProps<{
  contributor: Contributor,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsOverview',
};
</script>
