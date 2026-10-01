<template>
  <lf-organization-details-community :organization="props.organization" class="mb-10" />
  <lf-organization-details-attributes :organization="props.organization" />
</template>

<script setup lang="ts">
import type { Organization } from '@/modules/organization/types/Organization';
import LfOrganizationDetailsCommunity
  from '@/modules/organization/components/details/overview/organization-details-community.vue';
import LfOrganizationDetailsAttributes
  from '@/modules/organization/components/details/overview/organization-details-attributes.vue';

const props = defineProps<{
  organization: Organization,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsOverview',
};
</script>
