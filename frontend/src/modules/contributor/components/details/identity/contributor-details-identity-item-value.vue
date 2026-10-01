<template>
  <div class="flex items-center">
    <p v-if="!props.identity.url" class="text-medium max-w-48 truncate">
      {{ props.identity.value }}
    </p>
    <a
      v-else
      :href="props.identity.url"
      target="_blank"
      rel="noopener noreferrer"
      class="text-medium cursor-pointer !text-black underline decoration-dashed
      decoration-gray-400 underline-offset-4 hover:decoration-gray-900 max-w-48 truncate"
    >
      {{ props.identity.value }}
    </a>
    <p
      v-if="
        !lfIdentities[props.identity.platform]
          && !props.identity.platforms
          && getPlatformsLabel([props.identity.platform]).length > 0
      "
      class="text-medium text-gray-400 ml-1"
    >
      <span v-html="$sanitize(getPlatformsLabel([props.identity.platform]))" />
    </p>
  </div>
</template>

<script lang="ts" setup>
import type { ContributorIdentity } from '@/modules/contributor/types/Contributor';
import { lfIdentities } from '@/config/identities';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';

const props = defineProps<{
  identity: ContributorIdentity;
}>();

const { getPlatformsLabel } = useIdentitiesHelpers();
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsIdentityItemValue',
};
</script>
