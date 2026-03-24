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
          <lf-input v-model="form.value" class="h-10 flex-grow" :placeholder="`${platform?.member?.placeholder || ''}...`">
            <template #prefix>
              <div class="flex items-center flex-nowrap whitespace-nowrap">
                <div class="min-w-5">
                  <lf-icon v-if="form.type === 'email'" name="envelope" type="regular" class="text-black" :size="20" />
                  <img
                    v-else-if="platform"
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
                  v-if="form.type !== 'email' && platform?.member?.urlPrefix"
                  class="-mr-2 pl-2"
                  :class="form.value?.length ? 'text-black' : 'text-gray-400'"
                >
                  {{ platform?.member?.urlPrefix }}
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
import {
  computed, h, reactive, ref,
} from 'vue';
import { Contributor, ContributorIdentity } from '@/modules/contributor/types/Contributor';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';

import { ToastStore } from '@/shared/message/notification';
import { email, required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import { useMemberStore } from '@/modules/member/store/pinia';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  modelValue: ContributorIdentity,
  contributor: Contributor,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: ContributorIdentity | null): void}>();

const { updateContributorIdentity } = useContributorStore();
const memberStore = useMemberStore();

const sending = ref<boolean>(false);

const defaultForm: ContributorIdentity = {
  id: '',
  value: '',
  verified: false,
  platform: 'custom',
  type: 'username',
  source: 'ui',
  sourceId: null,
};

const form = reactive<ContributorIdentity>({
  ...defaultForm,
  ...props.modelValue,
});

const rules = {
  form: {
    value: {
      required,
      email: props.modelValue?.type === 'email' ? email : undefined,
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

const platform = computed(() => lfIdentities[form.platform]);

const updateIdentity = () => {
  sending.value = true;

  updateContributorIdentity(props.contributor.id, props.modelValue.id, {
    ...form,
    verified: false,
    source: 'ui',
    integrationId: null,
    sourceId: null,
    platform: form.type === 'email' ? 'custom' : form.platform,
  })
    .then(() => {
      ToastStore.success('Identity updated successfully');
      isModalOpen.value = false;
    })
    .catch((error) => {
      if (error.response.status === 409) {
        isModalOpen.value = false;
        ToastStore.success(
          h(
            'div',
            {
              class: 'flex flex-col gap-2',
            },
            [
              h(
                'button',
                {
                  class: 'c-btn c-btn--tiny c-btn--secondary-gray !h-6 !w-fit',
                  onClick: () => {
                    const { memberId, grandParentId } = error.response.data;

                    memberStore.addToMergeMember(memberId, grandParentId);
                    ToastStore.closeAll();
                  },
                },
                'Merge profiles',
              ),
            ],
          ),
          {
            title: 'Profile was not updated because the identity already exists in another profile, but you can merge the profiles.',
          },
        );
      } else {
        ToastStore.error('Something went wrong while editing an identity');
      }
    })
    .finally(() => {
      sending.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfContributorIdentityEdit',
};
</script>
