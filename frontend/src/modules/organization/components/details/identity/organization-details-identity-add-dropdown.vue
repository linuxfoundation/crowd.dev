<template>
  <lf-dropdown v-bind="$attrs" width="232px">
    <template #trigger>
      <slot />
    </template>
    <lf-dropdown-item
      v-for="platform of organizationIdentities"
      :key="platform.key"
      @click="emit('add', {
        platform: platform.key,
      })"
    >
      <div class="w-full flex items-center gap-2">
        <img :src="platform.image" :alt="platform.key" class="h-4 min-w-4 object-contain" /> {{ platform.name || platform.key }}
      </div>
    </lf-dropdown-item>
  </lf-dropdown>
</template>

<script setup lang="ts">
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import type { OrganizationIdentity } from '@/modules/organization/types/Organization';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';

const emit = defineEmits<{(e: 'add', value: Partial<OrganizationIdentity>): void}>();

const { organizationIdentities } = useIdentitiesHelpers();
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsIdentityAddDropdown',
};
</script>
