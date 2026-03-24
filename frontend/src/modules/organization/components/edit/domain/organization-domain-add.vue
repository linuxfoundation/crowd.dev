<template>
  <lf-modal v-model="isModalOpen">
    <template #default="{ close }">
      <div class="px-6 pt-4 pb-10">
        <div class="flex items-center justify-between pb-6">
          <h5>Add {{ modelValue?.type?.replace('-', ' ') }}</h5>
          <lf-button type="secondary-ghost-light" :icon-only="true" @click="close">
            <lf-icon name="xmark" />
          </lf-button>
        </div>

        <div class="flex flex-col gap-3 items-start">
          <article v-for="(domain, di) of form" :key="di" class="w-full">
            <lf-field class="w-full">
              <div class="flex items-center w-full">
                <lf-input
                  v-model="domain.value"
                  class="h-10 flex-grow"
                  placeholder="Enter URL"
                  :invalid="$v.form[di].value.$invalid && $v.form[di].value.$dirty"
                  @blur="$v.form[di].value.$touch()"
                >
                  <template #prefix>
                    <lf-icon name="link" class="text-black" :size="20" />
                  </template>
                </lf-input>
                <lf-button
                  v-if="form.length > 1"
                  class="ml-3"
                  type="secondary-ghost-light"
                  :icon-only="true"
                  @click="form.splice(di, 1)"
                >
                  <lf-icon name="trash-can" />
                </lf-button>
              </div>
              <lf-field-messages
                :validation="$v.form[di].value"
                :error-messages="{
                  required: 'This field is required',
                }"
              />
            </lf-field>
          </article>
          <lf-button
            type="primary-link"
            size="small"
            @click="form.push({
              ...defaultForm,
              ...props.modelValue,
            })"
          >
            <lf-icon name="plus" />
            Add domain
          </lf-button>
        </div>
      </div>
      <div class="py-4 px-6 border-t border-gray-100 flex items-center justify-end gap-4">
        <lf-button type="secondary-ghost" @click="close">
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          :loading="sending"
          :disabled="$v.$invalid || sending"
          @click="addDomains()"
        >
          Add {{ pluralize('domain', form.length) }}
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
import pluralize from 'pluralize';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfField from '@/ui-kit/field/Field.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import {
  Organization,
  OrganizationIdentity,
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import { Platform } from '@/shared/modules/platform/types/Platform';
import { useOrganizationStore } from '@/modules/organization/store/pinia';

const props = defineProps<{
  modelValue: Partial<OrganizationIdentity> | null,
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: Partial<OrganizationIdentity> | null): void}>();

const { updateOrganization } = useOrganizationStore();

const sending = ref<boolean>(false);

const defaultForm: OrganizationIdentity = {
  value: '',
  verified: false,
  platform: Platform.CUSTOM,
  type: OrganizationIdentityType.PRIMARY_DOMAIN,
  source: 'ui',
  sourceId: null,
};

const form = reactive<OrganizationIdentity[]>([
  {
    ...defaultForm,
    ...props.modelValue,
  },
]);

const rules = computed(() => ({
  form: form.map(() => ({
    value: {
      required,
    },
  })),
}));

const $v = useVuelidate(rules, { form });

const isModalOpen = computed<boolean>({
  get() {
    return !!props.modelValue;
  },
  set(value: boolean) {
    emit('update:modelValue', value ? props.modelValue : null);
  },
});

const addDomains = () => {
  sending.value = true;
  updateOrganization(props.organization.id, {
    identities: [
      ...props.organization.identities,
      ...form,
    ],
  })
    .then(() => {
      ToastStore.success('Domains successfully added');
      isModalOpen.value = false;
    })
    .catch(() => {
      ToastStore.error('Something went wrong while adding new domains');
    })
    .finally(() => {
      sending.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDomainAdd',
};
</script>
