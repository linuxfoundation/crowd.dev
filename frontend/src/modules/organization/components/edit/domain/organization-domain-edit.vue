<template>
  <lf-modal v-model="isModalOpen">
    <div class="px-6 py-4 flex items-center justify-between">
      <h5>
        Update {{ modelValue?.type?.replace('-', ' ') }}
      </h5>
      <lf-button type="secondary-ghost-light" :icon-only="true" @click="isModalOpen = false">
        <lf-icon name="xmark" />
      </lf-button>
    </div>
    <div class="pt-2 pb-10 px-6 w-full">
      <div class="flex items-center w-full">
        <lf-input
          v-model="form.value"
          class="!rounded-r-none h-10 flex-grow"
          placeholder="Enter URL"
        >
          <template #prefix>
            <div class="flex items-center flex-nowrap whitespace-nowrap">
              <div class="min-w-5">
                <lf-icon name="link" class="text-black" :size="20" />
              </div>
            </div>
          </template>
        </lf-input>
      </div>
    </div>
    <div class="py-4 px-6 border-t border-gray-100 flex items-center justify-end gap-4">
      <lf-button type="secondary-ghost" @click="isModalOpen = false">
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        :loading="sending"
        :disabled="$v.$invalid || !hasFormChanged || sending"
        @click="updateDomain()"
      >
        Update domain
      </lf-button>
    </div>
  </lf-modal>
</template>

<script lang="ts" setup>
import {
  Organization,
  OrganizationIdentity,
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import { computed, reactive, ref } from 'vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfInput from '@/ui-kit/input/Input.vue';

import { ToastStore } from '@/shared/message/notification';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { Platform } from '@/shared/modules/platform/types/Platform';

const props = defineProps<{
  modelValue: OrganizationIdentity | null,
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: OrganizationIdentity | null): void}>();

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

const form = reactive<OrganizationIdentity>({
  ...defaultForm,
  ...props.modelValue,
});

const rules = {
  form: {
    value: {
      required,
    },
  },
};

const $v = useVuelidate(rules, { form });

const hasFormChanged = computed(() => form.value !== props.modelValue?.value);

const isModalOpen = computed<boolean>({
  get: () => !!props.modelValue,
  set: (value: boolean) => {
    if (!value) {
      emit('update:modelValue', null);
    }
  },
});

const updateDomain = () => {
  const identities = props.organization.identities.map((i: OrganizationIdentity) => {
    if (i.type === props.modelValue?.type
        && i.platform === props.modelValue?.platform
        && i.value === props.modelValue?.value) {
      return {
        ...form,
        verified: false,
        source: 'ui',
        integrationId: null,
        sourceId: null,
        platform: 'custom',
      };
    }
    return i;
  });

  sending.value = true;

  updateOrganization(props.organization.id, {
    identities,
  })
    .then(() => {
      ToastStore.success('Domain updated successfully');
      isModalOpen.value = false;
    })
    .catch(() => {
      ToastStore.error('Something went wrong while editing a domain');
    })
    .finally(() => {
      sending.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDomainEdit',
};
</script>
