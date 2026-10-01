<template>
  <div v-if="member.organizations.length && currentOrganization.length" class="flex flex-col gap-3">
    <router-link
      v-if="currentOrganization[0]"
      :key="currentOrganization[0].id"
      :to="{
        name: 'organizationView',
        params: { id: currentOrganization[0].id },
        query: {
          projectGroup: selectedProjectGroup?.id,
          segmentId: member.segmentId,
        },
      }"
      class="flex items-start hover:cursor-pointer"
      @click.stop
    >
      <div class="w-6 h-6 min-w-[24px] mr-2 rounded-md overflow-hidden outline outline-1 outline-gray-200 flex items-center justify-center">
        <div v-if="currentOrganization[0].logo">
          <img :src="currentOrganization[0].logo" alt="Logo" class="w-full h-full" />
        </div>
        <lf-icon v-else name="house-building" :size="14" class="text-gray-300" />
      </div>
      <div class="max-w-full flex items-center gap-1">
        <p
          class="text-gray-900 text-sm line-clamp-1 font-medium underline decoration-dashed decoration-gray-400 underline-offset-4
    hover:decoration-gray-900 hover:cursor-pointer hover:!text-gray-900"
        >
          {{ currentOrganization[0].displayName || currentOrganization[0].name || '-' }}
        </p>
        <lf-organization-lf-member-tag
          :organization="currentOrganization[0]"
          only-show-icon
        />
      </div>
    </router-link>
  </div>
  <div v-else class="text-gray-900">
    -
  </div>
</template>

<script setup>
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { computed } from 'vue';
import LfOrganizationLfMemberTag from '@/modules/organization/components/lf-member/organization-lf-member-tag.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';

const props = defineProps({
  member: {
    type: Object,
    required: true,
  },
});

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
const { activeOrganization } = useContributorHelpers();

const currentOrganization = computed(() => activeOrganization(props.member));
</script>

<script>
export default {
  name: 'AppMemberOrganizations',
};
</script>
