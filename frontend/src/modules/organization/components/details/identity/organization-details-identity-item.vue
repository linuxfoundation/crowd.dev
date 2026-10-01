<template>
  <article
    class="flex items-start organization-identity-item"
    @mouseover="hovered = true"
    @mouseleave="hovered = false"
  >
    <div class="mt-0.5">
      <lf-tooltip
        v-if="lfIdentities[props.identity.platform]"
        placement="top-start"
        :content="lfIdentities[props.identity.platform].name"
      >
        <img
          :src="lfIdentities[props.identity.platform]?.image"
          class="h-5 min-w-5 object-contain"
          :alt="props.identity.value"
        />
      </lf-tooltip>
      <lf-tooltip v-else content="Custom identity" placement="top-start">
        <lf-icon
          name="fingerprint"
          :size="20"
          class="text-gray-600"
        />
      </lf-tooltip>
    </div>

    <div class="pl-3 flex-grow">
      <div class="flex items-center">
        <div class=" flex items-center">
          <p v-if="!props.identity.url" class="text-medium max-w-48 truncate">
            {{ props.identity.handle || props.identity.value }}
          </p>
          <a
            v-else
            :href="props.identity.url"
            target="_blank"
            rel="noopener noreferrer"
            class="text-medium cursor-pointer !text-black underline decoration-dashed
             decoration-gray-400 underline-offset-4 hover:decoration-gray-900 max-w-48 truncate"
          >
            {{ props.identity.handle ?? props.identity.value }}
          </a>
        </div>
        <lf-verified-identity-badge v-if="props.identity.verified" />
      </div>
    </div>

    <!-- Dropdown -->
    <lf-dropdown v-show="hovered" placement="bottom-end" width="232px">
      <template #trigger>
        <lf-button type="secondary-ghost" size="small" :icon-only="true">
          <lf-icon name="ellipsis" type="regular" />
        </lf-button>
      </template>
      <!-- Edit identity -->
      <lf-dropdown-item
        v-if="hasPermission(LfPermission.organizationEdit)"
        class="w-full"
        @click="emit('edit')"
      >
        <lf-icon name="pen fa-sharp" />Edit identity
      </lf-dropdown-item>

      <!-- Unmerge -->
      <lf-dropdown-item
        v-if="hasPermission(LfPermission.organizationEdit)"
        @click="emit('unmerge')"
      >
        <lf-icon name="link-simple-slash" />Unmerge identity
      </lf-dropdown-item>

      <lf-dropdown-item
        @click="setReportDataModal({
          organization: props.organization,
          type: ReportDataType.IDENTITY,
          attribute: props.identity,
        })"
      >
        <lf-icon name="message-exclamation" class="!text-red-500" />Report issue
      </lf-dropdown-item>

      <lf-dropdown-separator
        v-if="hasPermission(LfPermission.organizationEdit)"
      />
      <lf-dropdown-item
        v-if="hasPermission(LfPermission.organizationEdit)"
        type="danger"
        class="w-full"
        @click="removeIdentity"
      >
        <lf-icon name="trash-can" />Delete identity
      </lf-dropdown-item>
    </lf-dropdown>
  </article>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';

import { ToastStore } from '@/shared/message/notification';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { ref } from 'vue';
import type { Organization, OrganizationIdentity } from '@/modules/organization/types/Organization';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import LfVerifiedIdentityBadge from '@/shared/modules/identities/components/verified-identity-badge.vue';
import { useSharedStore } from '@/shared/pinia/shared.store';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  identity: OrganizationIdentity,
  organization: Organization
}>();

const emit = defineEmits<{(e: 'edit'): void, (e: 'unmerge'): void }>();

const { hasPermission } = usePermissions();
const { setReportDataModal } = useSharedStore();

const { updateOrganization } = useOrganizationStore();

const hovered = ref(false);

const removeIdentity = () => {
  const identities = props.organization.identities
    .filter((i: OrganizationIdentity) => !(i.platform === props.identity?.platform && i.value === props.identity?.value));

  updateOrganization(props.organization.id, {
    identities,
  })
    .then(() => {
      ToastStore.success('Identity deleted successfully');
    })
    .catch(() => {
      ToastStore.error('Something went wrong while deleting an identity');
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsIdentityItem',
};
</script>
