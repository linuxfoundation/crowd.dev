<template>
  <article
    @mouseover="hovered = true"
    @mouseleave="hovered = false"
  >
    <div class="flex min-h-7 relative">
      <div class="flex flex-auto flex-col overflow-hidden">
        <slot name="above" />
        <div v-if="props.organization?.memberOrganizations?.title" class="text-small text-gray-900 mb-1.5 flex items-center gap-1.5">
          <lf-svg name="id-card" class="h-4 w-4 text-gray-400" />
          <p class="truncate" style="max-width: 26ch">
            {{ props.organization?.memberOrganizations?.title }}
          </p>
        </div>
        <p class="text-small text-gray-500 mb-1.5 flex items-center">
          <lf-icon name="calendar" :size="16" class="mr-1.5 text-gray-400" />
          {{ getDateRange(props.organization?.memberOrganizations?.dateStart, props.organization?.memberOrganizations?.dateEnd) }}
        </p>
      </div>

      <div>
        <lf-dropdown v-show="hovered" placement="bottom-end" width="14.5rem">
          <template #trigger>
            <lf-button type="secondary-ghost" size="small" :icon-only="true">
              <lf-icon name="ellipsis" />
            </lf-button>
          </template>

          <template v-if="hasPermission(LfPermission.memberEdit)">
            <lf-dropdown-item @click="emit('edit')">
              <lf-icon name="pen fa-sharp" />Edit work experience
            </lf-dropdown-item>
            <lf-dropdown-separator />

            <lf-dropdown-item
              v-if="props.organization.memberOrganizations.affiliationOverride.isPrimaryWorkExperience"
              @click="setAffiliation({
                isPrimaryWorkExperience: false,
                allowAffiliation: false,
              })"
            >
              <lf-icon name="xmark-circle" type="regular" />Remove affiliation
            </lf-dropdown-item>
            <template v-else>
              <lf-tooltip
                v-if="!props.organization.memberOrganizations.affiliationOverride.isPrimaryWorkExperience"
                class="!w-full"
                placement="right"
                :disabled="!isOverlapping"
                content="You cannot affiliate an organization/job title that overlaps with another for the same time period"
              >
                <lf-dropdown-item
                  v-if="!props.organization.memberOrganizations.affiliationOverride.isPrimaryWorkExperience"
                  :disabled="isOverlapping"
                  @click="setAffiliation({
                    isPrimaryWorkExperience: true,
                  })"
                >
                  <lf-icon name="link" type="regular" />Mark as affiliated
                </lf-dropdown-item>
              </lf-tooltip>

              <lf-tooltip
                v-if="props.organization.memberOrganizations.affiliationOverride.allowAffiliation"
                class="!w-full"
                placement="right"
                content="Prevents this work experience from being considered for affiliations"
              >
                <lf-dropdown-item
                  @click="setAffiliation({
                    allowAffiliation: false,
                  })"
                >
                  <lf-icon name="ban" type="regular" />Block affiliations
                </lf-dropdown-item>
              </lf-tooltip>

              <lf-tooltip
                v-else
                class="!w-full"
                placement="right"
                content="Include this work experience for consideration in future affiliations"
              >
                <lf-dropdown-item
                  @click="setAffiliation({
                    allowAffiliation: true,
                  })"
                >
                  <lf-icon name="toggle-on" type="regular" />Enable affiliations
                </lf-dropdown-item>
              </lf-tooltip>
            </template>
          </template>

          <lf-dropdown-separator />

          <lf-dropdown-item
            @click="setReportDataModal({
              contributor: props.contributor,
              type: ReportDataType.WORK_EXPERIENCE,
              attribute: props.organization,
            })"
          >
            <lf-icon name="message-exclamation" class="!text-red-500" />Report issue
          </lf-dropdown-item>
          <template v-if="hasPermission(LfPermission.memberEdit)">
            <lf-dropdown-separator />
            <lf-dropdown-item type="danger" @click="removeWorkHistory">
              <lf-icon name="trash-can" />Delete work experience
            </lf-dropdown-item>
          </template>
        </lf-dropdown>
      </div>
    </div>
  </article>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import LfSvg from '@/shared/svg/svg.vue';
import type { Organization } from '@/modules/organization/types/Organization';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import { computed, ref } from 'vue';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';

import { ToastStore } from '@/shared/message/notification';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { useSharedStore } from '@/shared/pinia/shared.store';
import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import { ContributorAffiliationsApiService } from '@/modules/contributor/services/contributor.affiliations.api.service';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  organization: Organization,
  contributor: Contributor,
}>();

const emit = defineEmits<{(e:'edit'): void}>();

const { deleteContributorOrganization, getContributorOrganizations } = useContributorStore();
const { trackEvent } = useProductTracking();

const { hasPermission } = usePermissions();
const { setReportDataModal } = useSharedStore();

const hovered = ref<boolean>(false);

const getDateRange = (dateStart?: string, dateEnd?: string) => {
  const start = dateStart
    ? dateHelper(dateStart).utc().format('MMMM YYYY')
    : 'Unknown';
  const endDefault = dateStart ? 'Present' : 'Unknown';
  const end = dateEnd
    ? dateHelper(dateEnd).utc().format('MMMM YYYY')
    : endDefault;
  if (start === end) {
    return start;
  }
  return `${start} → ${end}`;
};

const restOrganizations = computed(() => props.contributor.organizations.filter((org) => org.id !== props.organization.id
      || org.memberOrganizations.title !== props.organization.memberOrganizations.title
      || org.memberOrganizations.dateStart !== props.organization.memberOrganizations.dateStart
      || org.memberOrganizations.dateEnd !== props.organization.memberOrganizations.dateEnd));

const isOverlapping = computed(() => {
  const org = props.organization.memberOrganizations;
  const dateStart = dateHelper(org.dateStart || 0);
  // undefined represents current date
  const dateEnd = dateHelper(org.dateEnd || undefined);
  return restOrganizations.value.some((o) => {
    if (!o.memberOrganizations.affiliationOverride.isPrimaryWorkExperience) {
      return false;
    }
    const oOrg = o.memberOrganizations;
    const dateStartCompare = dateHelper(oOrg.dateStart || 0);
    // undefined represents current date
    const dateEndCompare = dateHelper(oOrg.dateEnd || undefined);

    return dateStartCompare.isBefore(dateEnd) && dateEndCompare.isAfter(dateStart);
  });
});

const setAffiliation = (data: {
  isPrimaryWorkExperience?: boolean
  allowAffiliation?: boolean
}) => {
  ContributorAffiliationsApiService.updateAffiliationOverride(props.contributor.id, {
    isPrimaryWorkExperience: data.isPrimaryWorkExperience,
    allowAffiliation: data.allowAffiliation,
    memberOrganizationId: props.organization.memberOrganizations.id,
    memberId: props.contributor.id,
  })
    .then(() => {
      if (data.isPrimaryWorkExperience) {
        ToastStore.success('Organization/job title successfully affiliated');
      } else {
        ToastStore.success('Organization/job title affiliation successfully removed');
      }
      getContributorOrganizations(props.contributor.id);
    });
};

const removeWorkHistory = () => {
  ConfirmDialog({
    type: 'danger',
    title: 'Delete work experience',
    message: "Are you sure you want to proceed? You can't undo this action",
    confirmButtonText: 'Confirm',
    cancelButtonText: 'Cancel',
    icon: 'fa-light fa-trash-can',
  }).then(() => {
    trackEvent({
      key: FeatureEventKey.DELETE_WORK_EXPERIENCE,
      type: EventType.FEATURE,
      properties: {
      },
    });

    deleteContributorOrganization(props.contributor.id, props.organization.memberOrganizations.id)
      .then(() => {
        ToastStore.success('Work experience deleted successfully');
      })
      .catch(() => {
        ToastStore.error('Something went wrong while deleting an work experience');
      });
  });
};
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsWorkHistoryItem',
};
</script>
