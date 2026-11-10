<template>
  <app-identities-horizontal-list
    :identities="identities.getIdentities()"
    :limit="limit"
    :as-svg="asSvg"
  />
</template>

<script setup lang="ts">
import { computed } from 'vue';
import useOrganizationIdentities from '@/shared/modules/identities/config/useOrganizationIdentities';
import organizationOrder from '@/shared/modules/identities/config/identitiesOrder/organization';
import AppIdentitiesHorizontalList from '@/shared/modules/identities/components/identities-horizontal-list.vue';
import type { Organization } from '@/modules/organization/types/Organization';

defineOptions({
  name: 'AppIdentitiesHorizontalListOrganizations',
});

const props = defineProps<{
  organization: Organization;
  limit?: number;
  asSvg?: boolean;
}>();

const identities = computed(() => useOrganizationIdentities({
  organization: props.organization,
  order: organizationOrder.list,
}));
</script>
