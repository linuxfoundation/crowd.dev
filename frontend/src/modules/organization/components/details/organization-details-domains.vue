<template>
  <section v-bind="$attrs">
    <div class="flex justify-between items-center pb-5">
      <h6 class="text-h6">
        Domains
      </h6>
      <lf-organization-details-domains-add-dropdown @add="add = $event">
        <lf-tooltip content="Add domain">
          <lf-button
            v-if="hasPermission(LfPermission.organizationEdit)"
            type="secondary"
            size="small"
            :icon-only="true"
          >
            <lf-icon name="plus" type="regular" />
          </lf-button>
        </lf-tooltip>
      </lf-organization-details-domains-add-dropdown>
    </div>
    <div class="flex flex-col gap-6">
      <lf-organization-details-domains-section
        title="Primary domain"
        :domains="primaryDomains(props.organization)"
        :organization="props.organization"
        @edit="edit = $event"
        @unmerge="unmerge"
      />
      <lf-organization-details-domains-section
        title="Alternative domain"
        :domains="alternativeDomains(props.organization)"
        :organization="props.organization"
        @edit="edit = $event"
        @unmerge="unmerge"
      />
    </div>

    <div v-if="!domains(props.organization).length" class="pt-2 flex flex-col items-center w-full">
      <lf-icon name="link" :size="40" class="text-gray-300" />
      <p class="text-center pt-3 text-medium text-gray-400">
        No domains
      </p>
    </div>
  </section>
  <app-organization-unmerge-dialog
    v-if="isUnmergeDialogOpen"
    v-model="isUnmergeDialogOpen"
    :selected-identity="selectedIdentity"
  />
  <lf-organization-domain-add
    v-if="add"
    v-model="add"
    :organization="props.organization"
    @update:model-value="add = null"
  />
  <lf-organization-domain-edit v-if="edit" v-model="edit" :organization="props.organization" />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { ref } from 'vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import type { Organization, OrganizationIdentity } from '@/modules/organization/types/Organization';
import LfOrganizationDetailsDomainsSection
  from '@/modules/organization/components/details/domains/organization-details-domains-section.vue';
import AppOrganizationUnmergeDialog from '@/modules/organization/components/organization-unmerge-dialog.vue';
import LfOrganizationDomainEdit from '@/modules/organization/components/edit/domain/organization-domain-edit.vue';
import LfOrganizationDetailsDomainsAddDropdown
  from '@/modules/organization/components/details/domains/organization-details-domains-add-dropdown.vue';
import LfOrganizationDomainAdd from '@/modules/organization/components/edit/domain/organization-domain-add.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';

const props = defineProps<{
  organization: Organization,
}>();

const { hasPermission } = usePermissions();

const {
  primaryDomains, alternativeDomains,
  domains,
} = useOrganizationHelpers();

const add = ref<Partial<OrganizationIdentity> | null>(null);
const edit = ref<OrganizationIdentity | null>(null);
const isUnmergeDialogOpen = ref<Organization | null>(null);
const selectedIdentity = ref<OrganizationIdentity | null>(null);

const unmerge = (identity: any) => {
  if (identity) {
    selectedIdentity.value = identity;
  }
  isUnmergeDialogOpen.value = props.organization as any;
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsDomains',
};
</script>
