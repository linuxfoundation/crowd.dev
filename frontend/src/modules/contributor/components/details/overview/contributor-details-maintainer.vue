<template>
  <div class="flex flex-wrap gap-x-2 gap-y-1 w-auto -mr-2">
    <lf-badge v-if="maintainer.length" type="secondary">
      Maintainer
    </lf-badge>
    <lf-badge v-if="contributorRoles.length" type="secondary">
      Contributor
    </lf-badge>
    <div v-if="!maintainer.length && !contributorRoles.length">
      -
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Contributor, ContributorMaintainerRole } from '@/modules/contributor/types/Contributor';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import { computed } from 'vue';

const props = defineProps<{
  contributor: Contributor,
}>();

const maintainer = computed<ContributorMaintainerRole[]>(
  () => props.contributor.maintainerRoles.filter((role) => role.role.includes('maintainer')),
);
const contributorRoles = computed<ContributorMaintainerRole[]>(
  () => props.contributor.maintainerRoles.filter((role) => role.role.includes('contributor')),
);
</script>
<script lang="ts">
export default {
  name: 'LfContributorDetailsMaintainer',
};
</script>
