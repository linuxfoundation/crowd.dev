<template>
  <div
    v-if="Object.keys({
      ...identities.getIdentities(),
      ...(includeDomains ? { domains: identities.getDomains() } : {}),
      ...(includePhoneNumbers ? { phoneNumbers: identities.getPhoneNumbers() } : {}),
    }).length"
  >
    <app-identities-vertical-list
      :identities="{
        ...identities.getIdentities(),
        ...(includeEmails ? { emails: identities.getEmails() } : {}),
        ...(includeDomains ? { domains: identities.getDomains() } : {}),
        ...(includePhoneNumbers ? { phoneNumbers: identities.getPhoneNumbers() } : {}),
      }"
      :x-padding="xPadding"
      :display-show-more="displayShowMore"
    />
  </div>
  <div v-else class="text-gray-400 mt-6 text-xs italic px-6">
    No identities
  </div>

  <slot :identities="identities" />
</template>

<script setup lang="ts">
import useOrganizationIdentities from '@/shared/modules/identities/config/useOrganizationIdentities';
import AppIdentitiesVerticalList from '@/shared/modules/identities/components/identities-vertical-list.vue';
import { computed } from 'vue';
import type { Platform } from '@/shared/modules/platform/types/Platform';
import type { Organization } from '@/modules/organization/types/Organization';

const props = defineProps<{
  organization: Organization;
  order: Platform[]
  xPadding?: number;
  displayShowMore?: boolean;
  includeEmails?: boolean;
  includeDomains?: boolean;
  includePhoneNumbers?: boolean;
}>();

const identities = computed(() => useOrganizationIdentities({
  organization: props.organization,
  order: props.order,
}));
</script>

<script lang="ts">
export default {
  name: 'AppIdentitiesVerticalListOrganizations',
};
</script>
