<template>
  <div class="flex items-center gap-1.5">
    <div
      v-for="(idents, platform) of platformIdentities"
      :key="platform"
      class="platform-wrapper cursor-pointer relative"
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
            <lf-icon name="arrow-up-right-from-square" :size="14" class="text-gray-400" />
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
            v-if="lfIdentities[platform]?.icon"
            :name="lfIdentities[platform]?.icon"
            :type="lfIdentities[platform]?.iconType"
            :size="20"
            :style="{ color: lfIdentities[platform]?.color }"
            class="platform-icon"
          />
        </a>
      </lf-tooltip>

      <div
        v-if="idents.length > 1"
        class="absolute w-58 top-full left-1/2 -translate-x-1/2 bg-white flex flex-col gap-1 p-1 shadow platform-popover
         transition z-30 rounded-md border border-gray-100 mt-2 overflow-y-auto overflow-x-hidden max-h-72"
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
          <lf-icon v-if="identity.url" name="arrow-up-right-from-square" :size="16" class="text-gray-300" />
        </a>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import {
  organizationDetailsHeaderProfilePlatforms,
} from '@/modules/organization/config/details-header-profile-platforms';
import type { Organization } from '@/modules/organization/types/Organization';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  organization: Organization,
}>();

const { identities } = useOrganizationHelpers();

const platformIdentities = computed(() => {
  const data = {};
  organizationDetailsHeaderProfilePlatforms.forEach((p) => { data[p] = []; });
  identities(props.organization).forEach((i) => {
    if (organizationDetailsHeaderProfilePlatforms.includes(i.platform)) {
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
  name: 'LfOrganizationDetailsHeaderProfiles',
};
</script>

<style scoped lang="scss">
.platform-icon:not(:hover){
  @apply text-gray-400 #{!important};
}

.platform-wrapper{
  .platform-popover{
    @apply opacity-0 invisible;
  }

  &:hover{
    .platform-popover{
      @apply opacity-100 visible;
    }
  }
}
</style>
