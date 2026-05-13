<template>
  <lf-modal v-model="isModalOpen">
    <template #default="{ close }">
      <div class="px-6 pt-4 pb-10">
        <div class="flex items-center justify-between pb-6">
          <h5>Edit identity</h5>
          <lf-button type="secondary-ghost-light" :icon-only="true" @click="close">
            <lf-icon name="xmark" />
          </lf-button>
        </div>

        <div class="flex items-center">
          <lf-input v-model="form.value" class="h-10 flex-grow" placeholder="...">
            <template #prefix>
              <div class="flex items-center flex-nowrap whitespace-nowrap">
                <div class="min-w-5">
                  <img
                    v-if="platform"
                    :src="platform?.image"
                    class="h-5 min-w-5 object-contain"
                    :alt="form.value"
                  />
                  <lf-icon
                    v-else
                    name="fingerprint"
                    :size="20"
                    class="text-gray-600"
                  />
                </div>
                <p
                  v-if="platform?.organization?.urlPrefix"
                  class="-mr-2 pl-2"
                  :class="form.value?.length ? 'text-black' : 'text-gray-400'"
                >
                  {{ platform?.organization?.urlPrefix }}
                </p>
              </div>
            </template>
          </lf-input>
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
          @click="updateIdentity()"
        >
          Update identity
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
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import {
  Organization,
  OrganizationIdentity,
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { Platform } from '@/shared/modules/platform/types/Platform';
import { lfIdentities } from '@/config/identities';

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
  type: OrganizationIdentityType.USERNAME,
  source: 'ui',
  sourceId: null,
};

const value = {
  ...defaultForm,
  ...props.modelValue,
};

if (value.platform === Platform.LINKEDIN) {
  value.value = value.value.replace('company:', '');
}

const form = reactive<OrganizationIdentity>(value);

const rules = {
  form: {
    value: {
      required,
    },
  },
};

const $v = useVuelidate(rules, { form });

const hasFormChanged = computed(() => form.value !== props.modelValue?.value || form.verified !== props.modelValue?.verified);

const isModalOpen = computed<boolean>({
  get() {
    return props.modelValue !== null;
  },
  set(value: boolean) {
    emit('update:modelValue', value ? props.modelValue : null);
  },
});

const platform = computed(() => lfIdentities[form.platform]);

const updateIdentity = () => {
  const identities = props.organization.identities.map((i: OrganizationIdentity) => {
    if (i.platform === props.modelValue?.platform && i.value === props.modelValue?.value) {
      return {
        ...form,
        value: form.platform === Platform.LINKEDIN ? `company:${form.value}` : form.value,
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
      ToastStore.success('Identity updated successfully');
      isModalOpen.value = false;
    })
    .catch(() => {
      ToastStore.error('Something went wrong while editing an identity');
    })
    .finally(() => {
      sending.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationIdentityEdit',
};
</script>
