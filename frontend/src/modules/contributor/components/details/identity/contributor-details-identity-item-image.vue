<template>
  <lf-tooltip
    v-if="props.identity.type === 'email'"
    content="Email"
    placement="top-start"
  >
    <lf-icon name="envelope" type="regular" :size="20" />
  </lf-tooltip>
  <lf-tooltip
    v-else-if="lfIdentities[props.identity.platform]"
    placement="top-start"
    :content="lfIdentities[props.identity.platform].name"
  >
    <img
      :src="lfIdentities[props.identity.platform].image"
      class="h-5 min-w-5 object-contain"
      :alt="props.identity.value"
    />
  </lf-tooltip>
  <lf-tooltip v-else content="Custom identity" placement="top-start">
    <lf-icon name="fingerprint" :size="20" class="text-gray-600" />
  </lf-tooltip>
</template>

<script lang="ts" setup>
import type { ContributorIdentity } from '@/modules/contributor/types/Contributor';
import { lfIdentities } from '@/config/identities';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  identity: ContributorIdentity
}>();

</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsIdentityItemImage',
};
</script>
