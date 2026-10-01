<template>
  <article
    class="flex items-start justify-between w-full"
    @mouseover="hovered = true"
    @mouseleave="hovered = false"
  >
    <div class="flex items-start">
      <lf-icon name="envelope" :size="20" class="text-gray-500" />
      <div class="pl-2">
        <div class="flex items-center">
          <lf-tooltip
            :content="props.email.value"
            :disabled="props.email.value.length < 25"
          >
            <a
              :href="`mailto:${props.email.value}`"
              target="_blank"
              rel="noopener noreferrer"
              class="text-medium cursor-pointer !text-black underline decoration-dashed
             decoration-gray-400 underline-offset-4 hover:decoration-gray-900 truncate"
              style="max-width: 25ch"
            >
              {{ props.email.value }}
            </a>
          </lf-tooltip>
          <lf-tooltip v-if="props.email.verified" content="Verified email">
            <lf-icon
              name="badge-check"
              :size="16"
              class="ml-1 text-primary-500"
            />
          </lf-tooltip>
        </div>

        <p v-if="platformLabel(props.email.platforms).length > 0" class="mt-1.5 text-tiny text-gray-400">
          Source: <span v-html="$sanitize(platformLabel(props.email.platforms))" />
        </p>
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
        <lf-icon name="pen fa-sharp" />Edit email
      </lf-dropdown-item>
      <!-- Unmerge -->
      <lf-dropdown-item
        v-if="hasPermission(LfPermission.organizationEdit)"
        @click="emit('unmerge')"
      >
        <lf-icon name="link-simple-slash" />Unmerge email
      </lf-dropdown-item>

      <lf-dropdown-item
        @click="setReportDataModal({
          contributor: props.contributor,
          type: ReportDataType.WORK_EXPERIENCE,
          attribute: props.organization,
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
        @click="removeEmail"
      >
        <lf-icon name="trash-can" />Delete email
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
import type {
  Organization,
  OrganizationIdentity,
} from '@/modules/organization/types/Organization';
import {
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { useSharedStore } from '@/shared/pinia/shared.store';
import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';

const props = defineProps<{
  email: OrganizationIdentity,
  organization: Organization
}>();

const emit = defineEmits<{(e: 'edit'): void, (e: 'unmerge'): void }>();

const { hasPermission } = usePermissions();
const { setReportDataModal } = useSharedStore();

const { updateOrganization } = useOrganizationStore();
const { getPlatformsLabel } = useIdentitiesHelpers();

const hovered = ref(false);

const platformLabel = (platforms: string[]) => getPlatformsLabel(platforms);
const removeEmail = () => {
  const identities = props.organization.identities
    .filter((i: OrganizationIdentity) => !(i.type === OrganizationIdentityType.EMAIL && i.value === props.email?.value));

  updateOrganization(props.organization.id, {
    identities,
  })
    .then(() => {
      ToastStore.success('Email deleted successfully');
    })
    .catch(() => {
      ToastStore.error('Something went wrong while deleting an email');
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsEmailItem',
};
</script>
