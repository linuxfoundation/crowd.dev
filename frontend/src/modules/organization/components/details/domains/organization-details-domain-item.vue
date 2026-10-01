<template>
  <article
    @mouseover="hovered = true"
    @mouseleave="hovered = false"
  >
    <div class="flex items-center justify-between min-h-7">
      <div class="flex items-center">
        <lf-icon name="link" :size="20" class="text-gray-500 mr-2" />
        <lf-tooltip
          :content="props.domain.value"
          :disabled="props.domain.value.length < 25"
        >
          <a
            :href="withHttp(props.domain.value)"
            target="_blank"
            rel="noopener noreferrer"
            class="text-medium cursor-pointer !text-black underline decoration-dashed
             decoration-gray-400 underline-offset-4 hover:decoration-gray-900 truncate"
            style="max-width: 25ch"
          >
            {{ props.domain.value }}
          </a>
        </lf-tooltip>
        <lf-tooltip v-if="props.domain.verified" content="Verified domain" class="ml-1.5">
          <lf-icon name="badge-check" :size="16" class="text-primary-500" />
        </lf-tooltip>
      </div>
      <lf-dropdown
        v-show="props.domain.type !== OrganizationIdentityType.AFFILIATED_PROFILE
          && hovered"
        placement="bottom-end"
        width="232px"
      >
        <template #trigger>
          <lf-button type="secondary-ghost" size="small" :icon-only="true">
            <lf-icon name="ellipsis" type="regular" />
          </lf-button>
        </template>

        <lf-dropdown-item
          v-if="hasPermission(LfPermission.memberEdit)"
          class="w-full"
          @click="emit('edit')"
        >
          <lf-icon name="pen fa-sharp" />Edit domain
        </lf-dropdown-item>

        <!-- Unmerge -->
        <lf-dropdown-item
          v-if="hasPermission(LfPermission.memberEdit)"
          @click="emit('unmerge')"
        >
          <lf-icon name="link-simple-slash" />Unmerge domain
        </lf-dropdown-item>

        <lf-dropdown-item
          @click="setReportDataModal({
            organization: props.organization,
            type: ReportDataType.DOMAIN,
            attribute: props.domain,
          })"
        >
          <lf-icon name="message-exclamation" class="!text-red-500" />Report issue
        </lf-dropdown-item>

        <lf-dropdown-separator
          v-if="hasPermission(LfPermission.memberEdit)"
        />
        <lf-dropdown-item
          v-if="hasPermission(LfPermission.memberEdit)"
          type="danger"
          class="w-full"
          @click="removeDomain()"
        >
          <lf-icon name="trash-can" />Delete domain
        </lf-dropdown-item>
      </lf-dropdown>
    </div>

    <div v-if="platformLabel(props.domain.platforms).length" class="pl-7">
      <p class="text-tiny text-gray-400">
        Source: <span v-html="$sanitize(platformLabel(props.domain.platforms))" />
      </p>
    </div>
  </article>
</template>

<script setup lang="ts">
import type {
  Organization,
  OrganizationIdentity,
} from '@/modules/organization/types/Organization';
import {
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { withHttp } from '@/utils/string';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { ref } from 'vue';
import { ToastStore } from '@/shared/message/notification';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import { useSharedStore } from '@/shared/pinia/shared.store';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';

const props = defineProps<{
  domain: OrganizationIdentity,
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'edit'): void, (e: 'unmerge'): void, }>();

const { getPlatformsLabel } = useIdentitiesHelpers();

const platformLabel = (platforms: string[]) => getPlatformsLabel(platforms);

const { hasPermission } = usePermissions();
const { setReportDataModal } = useSharedStore();
const { updateOrganization } = useOrganizationStore();

const hovered = ref(false);

const removeDomain = () => {
  const identities = props.organization.identities
    .filter((i: OrganizationIdentity) => !(i.platform === props.domain?.platform
        && i.value === props.domain?.value
        && i.type === props.domain?.type));

  updateOrganization(props.organization.id, {
    identities,
  })
    .then(() => {
      ToastStore.success('Domain deleted successfully');
    })
    .catch(() => {
      ToastStore.error('Something went wrong while deleting a domain');
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsDomainItem',
};
</script>
