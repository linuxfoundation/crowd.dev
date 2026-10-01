<template>
  <section v-bind="$attrs">
    <div class="flex justify-between items-center pb-6">
      <h6 class="text-h6">
        Identities
      </h6>
      <lf-organization-details-identity-add-dropdown
        v-if="hasPermission(LfPermission.organizationEdit)"
        placement="bottom-end"
        @add="addIdentity = true; addIdentityTemplate = $event"
      >
        <lf-tooltip content="Add identity">
          <lf-button
            type="secondary"
            size="small"
            :icon-only="true"
            class="my-1"
          >
            <lf-icon name="plus" type="regular" />
          </lf-button>
        </lf-tooltip>
      </lf-organization-details-identity-add-dropdown>
    </div>

    <div class="flex flex-col gap-4">
      <lf-organization-details-identity-item
        v-for="identity of identityList.slice(0, showMore ? identityList.length : 10)"
        :key="`${identity.platform}-${identity.value}`"
        :identity="identity"
        :organization="props.organization"
        class="min-h-7"
        @edit="editIdentity = identity"
        @unmerge="unmerge(identity)"
      />
      <div v-if="identityList.length === 0" class="pt-2 flex flex-col items-center w-full">
        <lf-icon name="fingerprint" :size="40" class="text-gray-300" />
        <p class="text-center pt-3 text-medium text-gray-400">
          No identities
        </p>
      </div>
    </div>

    <lf-button
      v-if="identityList.length > 10"
      type="primary-link"
      size="medium"
      class="mt-6"
      @click="showMore = !showMore"
    >
      Show {{ showMore ? 'less' : 'more' }}
    </lf-button>
  </section>
  <lf-organization-identity-add
    v-if="addIdentity && addIdentityTemplate !== null"
    v-model="addIdentity"
    :identities="[addIdentityTemplate]"
    :organization="props.organization"
  />
  <lf-organization-identity-edit
    v-if="editIdentity !== null"
    v-model="editIdentity"
    :organization="props.organization"
  />
  <app-organization-unmerge-dialog
    v-if="isUnmergeDialogOpen"
    v-model="isUnmergeDialogOpen"
    :selected-identity="selectedIdentity"
  />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { computed, ref } from 'vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import type { Organization, OrganizationIdentity } from '@/modules/organization/types/Organization';
import AppOrganizationUnmergeDialog from '@/modules/organization/components/organization-unmerge-dialog.vue';
import LfOrganizationDetailsIdentityAddDropdown
  from '@/modules/organization/components/details/identity/organization-details-identity-add-dropdown.vue';
import LfOrganizationDetailsIdentityItem
  from '@/modules/organization/components/details/identity/organization-details-identity-item.vue';
import LfOrganizationIdentityAdd from '@/modules/organization/components/edit/identity/organization-identity-add.vue';
import LfOrganizationIdentityEdit from '@/modules/organization/components/edit/identity/organization-identity-edit.vue';

const props = defineProps<{
  organization: Organization,
}>();

const { hasPermission } = usePermissions();

const { identities } = useOrganizationHelpers();

const identityList = computed(() => identities(props.organization));

const showMore = ref<boolean>(false);
const isUnmergeDialogOpen = ref(null);
const selectedIdentity = ref(null);

const addIdentity = ref<boolean>(false);
const addIdentityTemplate = ref<Partial<OrganizationIdentity> | null>(null);
const editIdentity = ref<OrganizationIdentity | null>(null);
const unmerge = (identity: any) => {
  if (identity) {
    selectedIdentity.value = identity;
  }
  isUnmergeDialogOpen.value = props.organization as any;
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsIdentities',
};
</script>
