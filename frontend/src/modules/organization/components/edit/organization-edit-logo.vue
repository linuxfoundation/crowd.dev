<template>
  <lf-modal v-model="isModalOpen">
    <template #default="{ close }">
      <div class="px-6 pt-6 pb-10 flex border-b border-gray-100">
        <div class="pr-5">
          <lf-avatar
            :size="80"
            :name="displayName(props.organization)"
            :src="form.logo"
            class="border border-gray-300 !rounded-md"
            img-class="!object-contain"
          >
            <template #placeholder>
              <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                <lf-icon name="house-building" :size="48" class="text-gray-300" />
              </div>
            </template>
          </lf-avatar>
        </div>
        <div class="flex-grow">
          <lf-field label-text="Image URL">
            <lf-input
              v-model="form.logo"
              :invalid="$v.logo.$invalid"
              @blur="$v.logo.$touch()"
              @update:model-value="$v.logo.$touch()"
            >
              <template #suffix>
                <lf-icon
                  v-if="form.logo"
                  name="circle-xmark"
                  :size="20"
                  class="text-gray-300 cursor-pointer"
                  @click="form.logo = ''"
                />
              </template>
            </lf-input>
            <lf-field-messages
              :validation="$v.logo"
              :error-messages="{
                url: 'This URL address is not valid',
              }"
            />
            <lf-field-message type="hint">
              Choose a 1:1 aspect ratio (square) to better fit your image.
            </lf-field-message>
          </lf-field>
        </div>
      </div>
      <div class="px-6 py-4 gap-4 flex items-center justify-end">
        <lf-button type="secondary-ghost" @click="close">
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          :disabled="$v.$invalid || logo(props.organization) === form.logo"
          :loading="sending"

          @click="update()"
        >
          Update logo
        </lf-button>
      </div>
    </template>
  </lf-modal>
</template>

<script setup lang="ts">
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import { computed, reactive, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { url } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfInput from '@/ui-kit/input/Input.vue';
import LfField from '@/ui-kit/field/Field.vue';
import LfFieldMessage from '@/ui-kit/field-message/FieldMessage.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';

import { ToastStore } from '@/shared/message/notification';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Organization } from '@/modules/organization/types/Organization';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';

const props = defineProps<{
  modelValue: boolean,
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): any}>();

const { updateOrganization } = useOrganizationStore();

const { displayName, logo } = useOrganizationHelpers();

const form = reactive({
  logo: logo(props.organization),
});

const rules = {
  logo: {
    url,
  },
};

const $v = useVuelidate(rules, form);

const sending = ref<boolean>(false);
const update = () => {
  sending.value = true;
  updateOrganization(props.organization.id, {
    attributes: {
      logo: {
        default: form.logo,
        custom: [form.logo],
      },
    },
  })
    .then(() => {
      ToastStore.success('Organization logo updated successfully!');
      isModalOpen.value = false;
    })
    .catch(() => {
      ToastStore.error('There was an error updating organization');
    })
    .finally(() => {
      sending.value = false;
    });
};

const isModalOpen = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set(isOpen: boolean) {
    emit('update:modelValue', isOpen);
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationEditLogo',
};
</script>
