<template>
  <div class="flex items-center">
    <lf-avatar
      :name="displayName(props.attribute)"
      :src="logo(props.attribute)"
      :size="24"
      class="!rounded-md border border-gray-200 min-w-6"
      img-class="!object-contain"
    >
      <template #placeholder>
        <div class="w-full h-full bg-gray-50 flex items-center justify-center">
          <lf-icon name="house-building" :size="16" class="text-gray-400" />
        </div>
      </template>
    </lf-avatar>
    <p class="text-medium pl-2">
      {{ displayName(props.attribute) }}
    </p>
  </div>
</template>

<script setup lang="ts">
import type { Organization } from '@/modules/organization/types/Organization';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';

const props = defineProps<{
  attribute: Organization,
}>();

const { displayName, logo } = useOrganizationHelpers();
</script>
