<template>
  <lf-modal v-model="isModalOpen" class="!overflow-visible">
    <template #default="{ close }">
      <div class="px-6 pt-4 pb-10">
        <div class="flex items-center justify-between pb-6">
          <h5>Add identity</h5>
          <lf-button type="secondary-ghost-light" :icon-only="true" @click="close">
            <lf-icon name="xmark" />
          </lf-button>
        </div>

        <div class="flex flex-col gap-3 items-start">
          <article v-for="(identity, ii) of form" :key="ii" class="w-full">
            <lf-field class="w-full">
              <div class="flex items-center w-full">
                <lf-input
                  v-model="identity.value"
                  class="h-10 flex-grow"
                  :placeholder="`${lfIdentities[identity.platform]?.member?.placeholder || ''}...`"
                  :invalid="$v.form[ii].value.$invalid && $v.form[ii].value.$dirty"
                  @blur="$v.form[ii].value.$touch()"
                >
                  <template #prefix>
                    <div class="flex items-center flex-nowrap whitespace-nowrap">
                      <div class="min-w-5">
                        <lf-icon v-if="identity.type === 'email'" name="envelope" type="regular" class="text-black" :size="20" />
                        <img
                          v-else-if="lfIdentities[identity.platform]"
                          :src="lfIdentities[identity.platform]?.image"
                          class="h-5 min-w-5 object-contain"
                          :alt="identity.value"
                        />
                        <lf-icon
                          v-else
                          name="fingerprint"
                          :size="20"
                          class="text-gray-600"
                        />
                      </div>
                      <p
                        v-if="identity.type !== 'email' && lfIdentities[identity.platform]?.member?.urlPrefix"
                        class="-mr-2 pl-2"
                        :class="identity.value?.length ? 'text-black' : 'text-gray-400'"
                      >
                        {{ lfIdentities[identity.platform]?.member?.urlPrefix }}
                      </p>
                    </div>
                  </template>
                </lf-input>
                <lf-button
                  v-if="form.length > 1"
                  class="ml-3"
                  type="secondary-ghost-light"
                  :icon-only="true"
                  @click="form.splice(ii, 1)"
                >
                  <lf-icon name="trash-can" />
                </lf-button>
              </div>
              <lf-field-messages
                :validation="$v.form[ii].value"
                :error-messages="{
                  required: 'This field is required',
                  email: 'This email address is not valid',
                }"
              />
            </lf-field>
          </article>
          <lf-contributor-details-identity-add-dropdown
            placement="bottom-start"
            @add="form.push({
              ...defaultForm,
              ...$event,
            })"
          >
            <lf-button type="primary-link" size="small">
              <lf-icon name="plus" />
              Add identity
            </lf-button>
          </lf-contributor-details-identity-add-dropdown>
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
          @click="addIdentities()"
        >
          Add {{ pluralize('identity', form.length) }}
        </lf-button>
      </div>
    </template>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import {
  computed, reactive, ref, h,
} from 'vue';
import { Contributor, ContributorIdentity } from '@/modules/contributor/types/Contributor';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';

import { ToastStore } from '@/shared/message/notification';
import LfContributorDetailsIdentityAddDropdown
  from '@/modules/contributor/components/details/identity/contributor-details-identity-add-dropdown.vue';
import pluralize from 'pluralize';
import { email, helpers, required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfField from '@/ui-kit/field/Field.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import { useMemberStore } from '@/modules/member/store/pinia';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  modelValue: boolean,
  identities: Partial<ContributorIdentity>[],
  contributor: Contributor,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void}>();

const memberStore = useMemberStore();
const { createContributorIdentities } = useContributorStore();

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

const form = reactive<ContributorIdentity[]>(props.identities.map((i) => ({ ...defaultForm, ...i })));

const rules = computed(() => ({
  form: form.map((item) => ({
    value: {
      required,
      email: item.type === 'email' ? email : helpers.withParams({ type: 'undefined' }, () => true),
    },
  })),
}));

const $v = useVuelidate(rules, { form });

const isModalOpen = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set(value: boolean) {
    emit('update:modelValue', value);
  },
});

const addIdentities = () => {
  sending.value = true;
  createContributorIdentities(props.contributor.id, form)
    .then(() => {
      ToastStore.success('Identities successfully added');
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
        ToastStore.error('Something went wrong while adding a new identity');
      }
    })
    .finally(() => {
      sending.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfContributorIdentityAdd',
};
</script>
