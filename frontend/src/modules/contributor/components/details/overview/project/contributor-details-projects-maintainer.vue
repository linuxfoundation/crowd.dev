<template>
  <div class="flex items-center gap-1">
    <lf-contributor-details-projects-maintainer-item
      v-if="maintainer.length"
      :maintainer-roles="maintainer"
    >
      <span>Maintainer</span>
      <lf-svg name="git-repository" class="w-3 h-3 text-gray-400" />
      <span class="text-gray-400">{{ maintainer.length }}</span>
    </lf-contributor-details-projects-maintainer-item>
    <lf-contributor-details-projects-maintainer-item
      v-if="contributor.length"
      :maintainer-roles="contributor"
    >
      <span>Contributor</span>
      <lf-svg name="git-repository" class="w-3 h-3 text-gray-400" />
      <span class="text-gray-400">{{ contributor.length }}</span>
    </lf-contributor-details-projects-maintainer-item>
    <div v-if="!maintainer.length && !contributor.length">
      -
    </div>
  </div>
</template>

<script setup lang="ts">
import LfSvg from '@/shared/svg/svg.vue';
import type { ContributorMaintainerRole } from '@/modules/contributor/types/Contributor';
import { computed } from 'vue';
import LfContributorDetailsProjectsMaintainerItem
  from '@/modules/contributor/components/details/overview/project/contributor-details-projects-maintainer-item.vue';

const props = defineProps<{
  maintainerRoles: ContributorMaintainerRole[],
}>();

const maintainer = computed<ContributorMaintainerRole[]>(() => (props.maintainerRoles || []).filter((role) => role.role.includes('maintainer')));
const contributor = computed<ContributorMaintainerRole[]>(() => (props.maintainerRoles || []).filter((role) => role.role.includes('contributor')));
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsProjectsMaintainer',
};
</script>
