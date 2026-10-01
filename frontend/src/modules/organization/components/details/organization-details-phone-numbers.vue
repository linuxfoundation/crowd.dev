<template>
  <section v-bind="$attrs">
    <div class="flex justify-between items-center pb-6">
      <h6 class="text-h6">
        Phone numbers
      </h6>
      <lf-button
        v-if="hasPermission(LfPermission.organizationEdit)"
        type="secondary"
        size="small"
        :icon-only="true"
        @click="edit = true"
      >
        <lf-icon name="pen fa-sharp" />
      </lf-button>
    </div>

    <div class="flex flex-wrap gap-2">
      <lf-tooltip
        v-for="number of phoneNumbersList.slice(0, showMore ? phoneNumbersList.length : 10)"
        :key="number"
        :content="number"
        :disabled="number.length <= 30"
      >
        <lf-badge
          type="secondary"
          class="truncate"
          style="max-width: 30ch"
        >
          {{ number }}
        </lf-badge>
      </lf-tooltip>

      <div v-if="phoneNumbersList.length === 0" class="pt-2 flex flex-col items-center w-full">
        <lf-icon name="phone" :size="40" class="text-gray-300" />
        <p class="text-center pt-3 text-medium text-gray-400">
          No phone numbers
        </p>
      </div>
    </div>

    <lf-button
      v-if="phoneNumbersList.length > 10"
      type="primary-link"
      size="medium"
      class="mt-6"
      @click="showMore = !showMore"
    >
      Show {{ showMore ? 'less' : 'more' }}
    </lf-button>
  </section>
  <app-organization-manage-phone-numbers-drawer
    v-if="edit"
    v-model="edit"
    :organization="props.organization"
    @update:model-value="emit('reload')"
    @reload="emit('reload')"
  />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { computed, ref } from 'vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import type { Organization } from '@/modules/organization/types/Organization';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import AppOrganizationManagePhoneNumbersDrawer
  from '@/modules/organization/components/organization-manage-phone-numbers-drawer.vue';

const props = defineProps<{
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'reload'): any}>();

const { hasPermission } = usePermissions();

const { phoneNumbers } = useOrganizationHelpers();

const phoneNumbersList = computed(() => phoneNumbers(props.organization));

const showMore = ref<boolean>(false);
const edit = ref<boolean>(false);
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsPhoneNumbers',
};
</script>
