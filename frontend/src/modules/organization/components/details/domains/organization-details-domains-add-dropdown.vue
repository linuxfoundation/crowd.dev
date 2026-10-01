<template>
  <lf-dropdown placement="bottom-end" v-bind="$attrs" width="14.5rem">
    <template #trigger>
      <slot />
    </template>
    <lf-dropdown-item
      v-for="domainType in domainTypes"
      :key="domainType"
      @click="emit('add', { type: domainType })"
    >
      <span class="first-letter:uppercase">{{ domainType.replace('-', ' ') }}</span>
    </lf-dropdown-item>
  </lf-dropdown>
</template>

<script setup lang="ts">
import type { OrganizationIdentity } from '@/modules/organization/types/Organization';
import { OrganizationIdentityType } from '@/modules/organization/types/Organization';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';

const emit = defineEmits<{(e: 'add', value: Partial<OrganizationIdentity>): void}>();

const domainTypes: OrganizationIdentityType[] = [
  OrganizationIdentityType.PRIMARY_DOMAIN,
  OrganizationIdentityType.ALTERNATIVE_DOMAIN,
];

</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsDomainsAddDropdown',
};
</script>
