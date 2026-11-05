<template>
  <app-drawer
    v-model="drawerModel"
    size="480px"
    title="Edit phone numbers"
    custom-class="emails-drawer"
  >
    <template #content>
      <div class="border-t border-gray-200 -mt-4 -mx-6 px-6 pt-5">
        <p class="text-sm font-medium text-gray-900 mb-2">
          Phone number
        </p>
        <app-organization-form-phone-number
          v-model="organizationModel"
          @update:model-value="hasFormChanged = true"
        />
      </div>
    </template>
    <template #footer>
      <div style="flex: auto">
        <lf-button
          type="bordered"
          size="medium"
          class="mr-3"
          @click="handleCancel"
        >
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          size="medium"
          :disabled="$v.$invalid || !hasFormChanged || loading"
          :loading="loading"
          @click="handleSubmit"
        >
          Update
        </lf-button>
      </div>
    </template>
  </app-drawer>
</template>

<script setup lang="ts">
import {
  ref,
  computed,
} from 'vue';
import { ToastStore } from '@/shared/message/notification';
import cloneDeep from 'lodash/cloneDeep';
import { OrganizationService } from '@/modules/organization/organization-service';
import useVuelidate from '@vuelidate/core';
import AppOrganizationFormPhoneNumber from '@/modules/organization/components/form/organization-form-phone-number.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfButton from '@/ui-kit/button/Button.vue';
import type { Organization } from '@/modules/organization/types/Organization';

defineOptions({ name: 'AppOrganizationManagePhoneNumbersDrawer' });

const props = withDefaults(defineProps<{
  modelValue?: boolean;
  organization: Organization;
}>(), {
  modelValue: false,
});

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'reload'): void;
}>();

const { trackEvent } = useProductTracking();

const drawerModel = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    emit('update:modelValue', value);
  },
});

const organizationModel = ref(cloneDeep({
  ...props.organization,
  phoneNumbers: props.organization.attributes.phoneNumber?.default || [],
}));
const loading = ref(false);

const $v = useVuelidate({}, organizationModel);

const hasFormChanged = ref(false);

const handleCancel = () => {
  emit('update:modelValue', false);
};

const handleSubmit = async () => {
  trackEvent({
    key: FeatureEventKey.EDIT_ORGANIZATION_PHONE_NUMBER,
    type: EventType.FEATURE,
    properties: {
      phoneNumbers: organizationModel.value.phoneNumbers.filter((p: string) => p.trim().length),
    },
  });

  loading.value = true;
  OrganizationService.update(props.organization.id, {
    attributes: {
      ...props.organization.attributes,
      phoneNumber: {
        ...props.organization.attributes.logo,
        default: organizationModel.value.phoneNumbers.filter((p: string) => p.trim().length),
        custom: [organizationModel.value.phoneNumbers.filter((p: string) => p.trim().length)],
      },
    },
  }).then(() => {
    emit('reload');
  }).catch((err) => {
    ToastStore.error(err.response.data);
  }).finally(() => {
    loading.value = false;
  });
  emit('update:modelValue', false);
};
</script>

<style lang="scss">
.identities-drawer {
  .el-form-item,
  .el-form-item__content {
    @apply mb-0;
  }
}
</style>
