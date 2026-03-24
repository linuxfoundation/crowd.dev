<template>
  <lf-modal v-model="isModalOpen">
    <template #default="{ close }">
      <div class="px-6 pt-4 pb-10">
        <div class="flex items-center justify-between pb-6">
          <h5>Edit email</h5>
          <lf-button type="secondary-ghost-light" :icon-only="true" @click="close">
            <lf-icon name="xmark" />
          </lf-button>
        </div>

        <div class="flex items-center">
          <lf-field class="w-full">
            <lf-input
              v-model="form.value"
              class="h-10 flex-grow"
              placeholder="..."
              :invalid="$v.form.value.$invalid && $v.form.value.$dirty"
              @blur="$v.form.value.$touch()"
            >
              <template #prefix>
                <div class="min-w-5">
                  <lf-icon name="envelope" class="text-black" :size="20" />
                </div>
              </template>
            </lf-input>
            <lf-field-messages
              :validation="$v.form.value"
              :error-messages="{
                required: 'This field is required',
                email: 'This email address is not valid',
              }"
            />
          </lf-field>
        </div>
      </div>
      <div class="py-4 px-6 border-t border-gray-100 flex items-center justify-end gap-4">
        <lf-button type="secondary-ghost" @click="close">
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          :loading="sending"
          :disabled="$v.$invalid || !hasFormChanged || sending"
          @click="updateEmail()"
        >
          Update email
        </lf-button>
      </div>
    </template>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import { computed, reactive, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfInput from '@/ui-kit/input/Input.vue';

import { ToastStore } from '@/shared/message/notification';
import { email, required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import {
  Organization,
  OrganizationIdentity,
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfField from '@/ui-kit/field/Field.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';

const props = defineProps<{
  modelValue: OrganizationIdentity | null,
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: OrganizationIdentity | null): void}>();

const { updateOrganization } = useOrganizationStore();

const sending = ref<boolean>(false);

const defaultForm: OrganizationIdentity = {
  value: '',
  verified: true,
  platform: Platform.CUSTOM,
  type: OrganizationIdentityType.EMAIL,
  source: 'ui',
  sourceId: null,
};

const value = {
  ...defaultForm,
  ...props.modelValue,
};

const form = reactive<OrganizationIdentity>(value);

const rules = {
  form: {
    value: {
      required,
      email,
    },
  },
};

const $v = useVuelidate(rules, { form });

const hasFormChanged = computed(() => form.value !== props.modelValue?.value);

const isModalOpen = computed<boolean>({
  get() {
    return props.modelValue !== null;
  },
  set(value: boolean) {
    emit('update:modelValue', value ? props.modelValue : null);
  },
});

const updateEmail = () => {
  const identities = props.organization.identities.map((i: OrganizationIdentity) => {
    if (i.type === props.modelValue?.type && i.value === props.modelValue?.value) {
      return {
        ...form,
        verified: false,
        source: 'ui',
        integrationId: null,
        sourceId: null,
      } as OrganizationIdentity;
    }
    return i;
  });

  sending.value = true;

  updateOrganization(props.organization.id, {
    identities,
  })
    .then(() => {
      ToastStore.success('Email updated successfully');
      isModalOpen.value = false;
    })
    .catch(() => {
      ToastStore.error('Something went wrong while editing an email');
    })
    .finally(() => {
      sending.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationEmailEdit',
};
</script>
