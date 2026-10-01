<template>
  <article
    class="flex items-start contributor-identity-item"
    @mouseover="hovered = true"
    @mouseleave="hovered = false"
  >
    <div class="mt-0.5">
      <lf-contributor-details-identity-item-image :identity="props.identity" />
    </div>

    <div class="pl-3 flex-grow">
      <div class="flex items-center relative">
        <lf-popover
          v-if="props.identity.duplicatedIdentities?.length"
          placement="top"
          trigger-event="hover"
        >
          <template #trigger>
            <lf-contributor-details-identity-item-value
              :identity="props.identity"
            />
          </template>
          <div v-for="duplicatedIdentity of props.identity.duplicatedIdentities" :key="duplicatedIdentity.id" class="flex items-center mb-2">
            <lf-contributor-details-identity-item-image :identity="duplicatedIdentity" />
            <lf-contributor-details-identity-item-value
              class="ml-3"
              :identity="duplicatedIdentity"
            />
          </div>
        </lf-popover>
        <lf-contributor-details-identity-item-value
          v-else
          :identity="props.identity"
        />
        <lf-verified-identity-badge v-if="props.identity.verified" />
      </div>
      <p
        v-if="props.identity.platforms && getPlatformsLabel(props.identity.platforms).length > 0"
        class="text-tiny text-gray-400 pt-1.5"
      >
        Source: <span v-html="$sanitize(getPlatformsLabel(props.identity.platforms))" />
      </p>
    </div>

    <!-- Dropdown -->
    <lf-dropdown v-show="hovered" placement="bottom-end" width="232px">
      <template #trigger>
        <lf-button type="secondary-ghost" size="small" :icon-only="true">
          <lf-icon name="ellipsis" />
        </lf-button>
      </template>
      <!-- Edit identity -->
      <lf-tooltip
        v-if="hasPermission(LfPermission.memberEdit)"
        placement="top"
        :disabled="!editingDisabled"
        class="!w-full"
      >
        <template #content>
          Identity can't be edited because the <br>contributor is active on
          {{ lfIdentities[props.identity.platform]?.name || props.identity.platform }}
        </template>
        <lf-dropdown-item
          :disabled="editingDisabled"
          class="w-full"
          @click="emit('edit')"
        >
          <lf-icon name="pen fa-sharp" />Edit identity
        </lf-dropdown-item>
      </lf-tooltip>

      <!-- Unmerge -->
      <lf-dropdown-item
        v-if="hasPermission(LfPermission.memberEdit)"
        @click="emit('unmerge')"
      >
        <lf-icon name="link-simple-slash" />Unmerge identity
      </lf-dropdown-item>

      <lf-dropdown-item
        @click="setReportDataModal({
          contributor: props.contributor,
          type: ReportDataType.IDENTITY,
          attribute: props.identity,
        })"
      >
        <lf-icon name="message-exclamation fa-sharp" class="!text-red-500" />Report issue
      </lf-dropdown-item>

      <template
        v-if="hasPermission(LfPermission.memberEdit)"
      >
        <lf-dropdown-separator />
        <lf-tooltip
          placement="top"
          :disabled="!editingDisabled"
          class="!w-full"
        >
          <template #content>
            Identity can't be deleted because the <br>contributor is active on
            {{ lfIdentities[props.identity.platform]?.name || props.identity.platform }}
          </template>
          <lf-dropdown-item
            type="danger"
            :disabled="editingDisabled"
            class="w-full"
            @click="removeIdentity"
          >
            <lf-icon name="trash-can" />Delete identity
          </lf-dropdown-item>
        </lf-tooltip>
      </template>
    </lf-dropdown>
  </article>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Contributor, ContributorIdentity } from '@/modules/contributor/types/Contributor';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';

import { ToastStore } from '@/shared/message/notification';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { computed, ref } from 'vue';
import LfVerifiedIdentityBadge from '@/shared/modules/identities/components/verified-identity-badge.vue';
import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import { useSharedStore } from '@/shared/pinia/shared.store';
import { lfIdentities } from '@/config/identities';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';
import LfPopover from '@/ui-kit/popover/Popover.vue';
import LfContributorDetailsIdentityItemImage from './contributor-details-identity-item-image.vue';
import LfContributorDetailsIdentityItemValue from './contributor-details-identity-item-value.vue';

const props = defineProps<{
  identity: ContributorIdentity,
  contributor: Contributor
}>();

const emit = defineEmits<{(e: 'edit'): void, (e: 'unmerge'): void }>();

const { hasPermission } = usePermissions();
const { setReportDataModal } = useSharedStore();
const { deleteContributorIdentity } = useContributorStore();
const { getPlatformsLabel } = useIdentitiesHelpers();

const hovered = ref(false);

const editingDisabled = computed(() => {
  if (['git'].includes(props.identity.platform)) {
    return false;
  }
  return props.contributor
    ? props.contributor.activeOn?.includes(props.identity.platform)
    : false;
});

const removeIdentity = () => {
  deleteContributorIdentity(props.contributor.id, props.identity.id)
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
  name: 'LfContributorDetailsIdentityItem',
};
</script>
