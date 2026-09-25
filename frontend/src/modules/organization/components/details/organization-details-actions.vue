<template>
  <div class="flex">
    <lf-button-group>
      <lf-button
        type="secondary"
        @click="setReportDataModal({
          organization: props.organization,
        })"
      >
        <lf-icon name="message-exclamation" class="text-red-500" /> Report data issue
      </lf-button>
      <!-- Merge suggestions -->
      <lf-button
        v-if="mergeSuggestionsCount > 0 && hasPermission(LfPermission.mergeOrganizations)"
        type="secondary"
        @click="isMergeSuggestionsDialogOpen = true"
      >
        <div class="bg-primary-500 text-white text-medium leading-5 px-1.5 rounded font-semibold">
          {{ mergeSuggestionsCount }}
        </div>
        {{ pluralize('Merge suggestion', mergeSuggestionsCount) }}
      </lf-button>

      <!-- Merge -->
      <lf-button v-else-if="hasPermission(LfPermission.mergeOrganizations)" type="secondary" @click="isMergeDialogOpen = props.organization">
        <lf-icon name="merge" />
        Merge organization
      </lf-button>

      <!-- Actions -->
      <lf-dropdown
        v-if="hasPermission(LfPermission.organizationEdit) || hasPermission(LfPermission.organizationDestroy)"
        class="z-20"
        placement="bottom-end"
      >
        <template #trigger>
          <lf-button
            type="secondary"
            :icon-only="true"
            :class="hasPermission(LfPermission.mergeOrganizations) ? '!rounded-l-none -ml-px' : ''"
          >
            <lf-icon name="ellipsis" type="regular" />
          </lf-button>
        </template>

        <lf-organization-dropdown
          :organization="props.organization"
          @reload="emit('reload')"
          @unmerge="unmerge = props.organization"
        />
      </lf-dropdown>
    </lf-button-group>
  </div>
  <app-organization-merge-suggestions-dialog
    v-if="isMergeSuggestionsDialogOpen"
    v-model="isMergeSuggestionsDialogOpen"
    :query="{
      filter: {
        organizationId: props.organization?.id,
      },
    }"
  />
  <app-organization-merge-dialog
    v-if="isMergeDialogOpen"
    v-model="isMergeDialogOpen"
  />

  <app-organization-unmerge-dialog
    v-if="unmerge"
    v-model="unmerge"
  />
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfButtonGroup from '@/ui-kit/button/ButtonGroup.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import { onMounted, ref } from 'vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { Organization } from '@/modules/organization/types/Organization';
import { OrganizationApiService } from '@/modules/organization/services/organization.api.service';
import AppOrganizationMergeSuggestionsDialog
  from '@/modules/organization/components/organization-merge-suggestions-dialog.vue';
import AppOrganizationMergeDialog from '@/modules/organization/components/organization-merge-dialog.vue';
import LfOrganizationDropdown from '@/modules/organization/components/shared/organization-dropdown.vue';
import pluralize from 'pluralize';
import { Contributor } from '@/modules/contributor/types/Contributor';
import { useSharedStore } from '@/shared/pinia/shared.store';
import AppOrganizationUnmergeDialog from '@/modules/organization/components/organization-unmerge-dialog.vue';

const props = defineProps<{
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'reload'): any}>();

const { hasPermission } = usePermissions();
const { setReportDataModal } = useSharedStore();

const isMergeSuggestionsDialogOpen = ref<boolean>(false);
const isMergeDialogOpen = ref<Contributor | null>(null);
const unmerge = ref<Organization | null>(null);
const mergeSuggestionsCount = ref<number>(0);

const fetchMergeSuggestions = () => {
  OrganizationApiService.fetchMergeSuggestions(0, 0, {
    filter: {
      organizationId: props.organization.id,
    },
    detail: false,
    countOnly: true,
  })
    .then(({ count }) => {
      mergeSuggestionsCount.value = +count;
    });
};

onMounted(() => {
  fetchMergeSuggestions();
});
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsActions',
};
</script>
