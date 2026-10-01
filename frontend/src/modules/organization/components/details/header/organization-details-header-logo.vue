<template>
  <div class="relative">
    <lf-avatar
      :size="48"
      :name="displayName(props.organization)"
      :src="logo(props.organization)"
      class="!rounded-md border border-gray-300 cursor-pointer"
      img-class="!object-contain"
      @click="isEditModalOpen = true"
    >
      <template #placeholder>
        <div class="w-full h-full bg-gray-50 flex items-center justify-center">
          <lf-icon name="house-building" :size="32" class="text-gray-300" />
        </div>
      </template>
      <template #overlay>
        <div class="absolute top-0 left-0 w-full h-full bg-secondary-500 opacity-50" />
        <div class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
          <lf-icon name="pen fa-sharp" :size="24" class="text-white" />
        </div>
      </template>
    </lf-avatar>
    <div
      v-if="isNew(props.organization)"
      class="absolute -top-1.5 left-1/2 border-2 border-white bg-primary-500
      text-xtiny rounded-md px-0.5 text-white font-semibold transform -translate-x-1/2 "
    >
      New
    </div>
  </div>
  <lf-organization-edit-logo
    v-if="isEditModalOpen"
    v-model="isEditModalOpen"
    :organization="props.organization"
  />
</template>

<script setup lang="ts">
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { ref } from 'vue';
import type { Organization } from '@/modules/organization/types/Organization';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import LfOrganizationEditLogo from '@/modules/organization/components/edit/organization-edit-logo.vue';

const props = defineProps<{
  organization: Organization,
}>();

const { isNew, logo, displayName } = useOrganizationHelpers();

const isEditModalOpen = ref<boolean>(false);
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsHeaderLogo',
};
</script>
