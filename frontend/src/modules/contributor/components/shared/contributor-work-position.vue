<template>
  <div v-if="jobTitle || organization" class="flex items-center gap-1.5 text-small text-gray-500">
    <p class="flex items-center">
      <span class="max-w-42 inline-block truncate">{{ jobTitle }}</span><span v-if="organization && jobTitle">&nbsp;at</span>
    </p>
    <lf-avatar
      :src="organization.logo"
      :name="organization.displayName"
      :size="18"
      class="!rounded border border-gray-200"
      img-class="object-contain"
    >
      <template #placeholder>
        <div class="w-full h-full bg-gray-50 flex items-center justify-center">
          <lf-icon name="house-building" :size="14" class="text-gray-400" />
        </div>
      </template>
    </lf-avatar>
    <router-link
      v-if="organization"
      :to="{
        name: 'organizationView',
        params: { id: organization.id },
        query: {
          projectGroup: selectedProjectGroup?.id,
          segmentId: props.contributor.segmentId,
        },
      }"
      class="cursor-pointer text-small leading-5 underline decoration-dashed text-gray-500
             decoration-gray-500 underline-offset-4 hover:decoration-gray-900 hover:!text-black max-w-30 truncate"
    >
      {{ organization.displayName }}
    </router-link>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';

const props = defineProps<{
  contributor: Contributor,
}>();

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const { activeOrganization } = useContributorHelpers();

const organization = computed(() => activeOrganization(props.contributor));
const jobTitle = computed(() => organization.value?.memberOrganizations?.title
    || props.contributor.attributes?.jobTitle?.default);
</script>

<script lang="ts">
export default {
  name: 'LfContributorWorkPosition',
};
</script>
