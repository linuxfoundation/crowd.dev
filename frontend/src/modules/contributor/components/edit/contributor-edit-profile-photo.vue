<template>
  <lf-modal v-model="isModalOpen">
    <template #default="{ close }">
      <div class="px-6 pt-6 pb-10 flex border-b border-gray-100">
        <div class="pr-5">
          <lf-avatar
            :size="80"
            :name="props.contributor.displayName"
            :src="form.profilePhoto"
          />
        </div>
        <div class="flex-grow">
          <lf-field label-text="Image URL">
            <lf-input
              v-model="form.profilePhoto"
              :invalid="$v.profilePhoto.$invalid"
              @blur="$v.profilePhoto.$touch()"
              @update:model-value="$v.profilePhoto.$touch()"
            >
              <template #suffix>
                <lf-icon
                  v-if="form.profilePhoto"
                  name="circle-xmark"
                  type="regular"
                  :size="20"
                  class="text-gray-300 cursor-pointer"
                  @click="form.profilePhoto = ''"
                />
              </template>
            </lf-input>
            <lf-field-messages
              :validation="$v.profilePhoto"
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
          :disabled="$v.$invalid || defaultAvatar === form.profilePhoto"
          :loading="sending"
          @click="update()"
        >
          Update avatar
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
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';
import type { Contributor } from '@/modules/contributor/types/Contributor';

const props = defineProps<{
  modelValue: boolean,
  contributor: Contributor,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): any}>();

const { avatar } = useContributorHelpers();
const { updateContributorAttributes } = useContributorStore();

const defaultAvatar = avatar(props.contributor);

const form = reactive({
  profilePhoto: avatar(props.contributor),
});

const rules = {
  profilePhoto: {
    url,
  },
};
const $v = useVuelidate(rules, form);

const sending = ref<boolean>(false);

const update = () => {
  sending.value = true;
  updateContributorAttributes(props.contributor.id, {
    ...props.contributor.attributes,
    avatarUrl: {
      ...props.contributor.attributes.avatarUrl,
      default: form.profilePhoto,
      custom: form.profilePhoto,
    },
  })
    .then(() => {
      ToastStore.success('Avatar updated successfully!');
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
  name: 'LfContributorEditProfilePhoto',
};
</script>
