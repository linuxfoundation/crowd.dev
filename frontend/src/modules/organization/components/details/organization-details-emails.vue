<template>
  <section v-bind="$attrs">
    <div class="flex justify-between items-center pb-6">
      <h6 class="text-h6">
        Emails
      </h6>
      <lf-tooltip v-if="hasPermission(LfPermission.organizationEdit)" content="Add email">
        <lf-button
          type="secondary"
          size="small"
          :icon-only="true"
          class="my-1"
          @click="addEmail = true"
        >
          <lf-icon name="plus" type="regular" />
        </lf-button>
      </lf-tooltip>
    </div>

    <div class="flex flex-wrap gap-3">
      <lf-organization-details-email-item
        v-for="email of emailList.slice(0, showMore ? emailList.length : limit)"
        :key="email.value"
        :email="email"
        :organization="props.organization"
        @edit="editEmail = email"
        @unmerge="unmerge(email)"
      />
    </div>
    <div>
      <div v-if="emailList.length === 0" class="pt-2 flex flex-col items-center w-full">
        <lf-icon name="at" :size="40" class="text-gray-300" />
        <p class="text-center pt-3 text-medium text-gray-400">
          No emails
        </p>
      </div>
    </div>

    <lf-button
      v-if="emailList.length > limit"
      type="primary-link"
      size="medium"
      class="mt-6"
      @click="showMore = !showMore"
    >
      Show {{ showMore ? 'less' : 'more' }}
    </lf-button>
  </section>
  <app-organization-unmerge-dialog
    v-if="isUnmergeDialogOpen"
    v-model="isUnmergeDialogOpen"
    :selected-identity="selectedIdentity"
  />
  <lf-organization-email-add
    v-if="addEmail"
    v-model="addEmail"
    :organization="props.organization"
  />
  <lf-organization-email-edit
    v-if="editEmail !== null"
    v-model="editEmail"
    :organization="props.organization"
  />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { computed, ref } from 'vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import type { Organization, OrganizationIdentity } from '@/modules/organization/types/Organization';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import AppOrganizationUnmergeDialog from '@/modules/organization/components/organization-unmerge-dialog.vue';
import LfOrganizationEmailAdd from '@/modules/organization/components/edit/email/organization-email-add.vue';
import LfOrganizationEmailEdit from '@/modules/organization/components/edit/email/organization-email-edit.vue';
import LfOrganizationDetailsEmailItem
  from '@/modules/organization/components/details/email/organization-details-email-item.vue';

const props = defineProps<{
  organization: Organization,
}>();

const limit = 10;

const { hasPermission } = usePermissions();

const { emails } = useOrganizationHelpers();

const addEmail = ref<boolean>(false);
const editEmail = ref<OrganizationIdentity | null>(null);

const emailList = computed(() => emails(props.organization));

const showMore = ref<boolean>(false);
const isUnmergeDialogOpen = ref(null);
const selectedIdentity = ref(null);
const unmerge = (identity: any) => {
  if (identity) {
    selectedIdentity.value = identity;
  }
  isUnmergeDialogOpen.value = props.organization as any;
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsEmails',
};
</script>
