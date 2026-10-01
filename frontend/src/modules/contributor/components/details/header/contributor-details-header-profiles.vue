<template>
  <div class="flex items-center gap-1.5">
    <div
      v-for="(idents, platform) of platformIdentities"
      :key="platform"
      class="group cursor-pointer relative"
    >
      <lf-tooltip :disabled="idents.length > 1 || !idents[0].url">
        <template #content>
          <a
            v-if="idents[0].url"
            :href="idents[0].url"
            target="_blank"
            rel="noopener noreferrer"
            class="flex items-center gap-1"
          >
            <p class="!text-white">
              {{ lfIdentities[platform]?.name }} profile
            </p>
            <lf-icon name="arrow-up-right-from-square" type="regular" :size="14" class="text-gray-400" />
          </a>
        </template>
        <a
          v-if="idents[0].url"
          :aria-label="platform"
          :href="idents[0].url"
          target="_blank"
          rel="noopener noreferrer"
          class="flex items-center gap-1"
          @click="idents.length > 1 ? $event.preventDefault() : null"
        >
          <lf-icon
            :name="lfIdentities[platform]?.icon || 'satellite-dish'"
            :type="lfIdentities[platform]?.iconType"
            :size="20"
            :style="{ color: lfIdentities[platform]?.color }"
            class="platform-icon"
          />
        </a>
      </lf-tooltip>

      <div
        v-if="idents.length > 1"
        class="absolute w-58 top-full left-1/2 -translate-x-1/2 bg-white flex flex-col gap-1 p-1 shadow
opacity-0 invisible transition group-hover:visible group-hover:opacity-100 z-30 rounded-md border border-gray-100 mt-2"
      >
        <div class="absolute -top-2 h-2 w-full left-0" />
        <a
          v-for="identity of idents"
          :key="`${platform}-${identity.value}`"
          :href="identity.url"
          class="flex items-center justify-between p-2 transition"
          :class="identity.url ? 'hover:bg-gray-50' : ''"
          target="_blank"
          rel="noopener noreferrer"
          @click="!identity.url ? $event.preventDefault() : null"
        >
          <div class="flex items-center text-gray-900">
            <img
              :alt="platform"
              :src="lfIdentities[platform]?.image"
              class="h-4 min-w-4"
            />
            <p class="pl-2 text-small">
              {{ identity.value }}
            </p>
          </div>
          <lf-icon v-if="identity.url" name="arrow-up-right-from-square" type="regular" :size="16" class="text-gray-300" />
        </a>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';
import { contributorDetailsHeaderProfilePlatforms } from '@/modules/contributor/config/details-header-profile-platforms';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  contributor: Contributor,
}>();

const { identities } = useContributorHelpers();

const platformIdentities = computed(() => {
  const data = {};
  contributorDetailsHeaderProfilePlatforms.forEach((p) => { data[p] = []; });
  identities(props.contributor).forEach((i) => {
    if (contributorDetailsHeaderProfilePlatforms.includes(i.platform)) {
      data[i.platform].push(i);
    }
  });
  Object.keys(data).forEach((platform) => {
    if (data[platform].length === 0) {
      delete data[platform];
    }
  });
  return data;
});
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsHeaderProfiles',
};
</script>

<style scoped lang="scss">
.platform-icon:not(:hover){
  @apply text-gray-400 #{!important};
}
</style>
