<template>
  <lf-modal v-model="isModalOpen">
    <!-- Header -->
    <section class="py-4 pr-4 pl-6 flex justify-between items-center">
      <h5>Add person</h5>
      <lf-button type="secondary-ghost-light" :icon-only="true" @click="isModalOpen = false">
        <lf-icon name="xmark" />
      </lf-button>
    </section>

    <!-- Subproject selection -->
    <section class="px-6 py-5 bg-primary-25">
      <div class="relative">
        <app-lf-sub-projects-list-dropdown
          @on-change="form.subproject = $event.subprojectId"
        />
      </div>
    </section>

    <!-- Form -->
    <section class="relative">
      <div v-if="!form.subproject" class="absolute left-0 top-0 w-full h-full bg-white opacity-50 z-20" />
      <lf-scroll-shadow class="max-h-120">
        <div class="px-6 py-5">
          <!-- Contributor name -->
          <article class="mb-5">
            <lf-field label-text="Name" :required="true">
              <lf-input
                v-model="form.name"
                class="h-10"
                :invalid="$v.name.$invalid && $v.name.$dirty"
                @blur="$v.name.$touch()"
                @change="$v.name.$touch()"
              />
              <lf-field-messages :validation="$v.name" :error-messages="{ required: 'This field is required' }" />
            </lf-field>
          </article>

          <!-- Contributor email -->
          <article class="mb-5">
            <lf-field label-text="Email address" :required="true">
              <div class="flex flex-col items-start gap-3">
                <lf-contributor-add-email-item
                  v-for="(_, ei) of form.email"
                  :key="`email:${ei}`"
                  v-model="form.email[ei]"
                  @blur="$v.email.$touch()"
                >
                  <div v-if="form.email.length > 1">
                    <lf-button
                      type="secondary-ghost-light"
                      size="large"
                      class="ml-2"
                      :icon-only="true"
                      @click="form.email.splice(ei, 1)"
                    >
                      <lf-icon name="trash-can" />
                    </lf-button>
                  </div>
                </lf-contributor-add-email-item>
              </div>
              <lf-field-messages
                :validation="$v.email"
                :error-messages="{ required: 'Enter at least one email', $each: '' }"
              />
            </lf-field>
            <lf-button type="primary-link" size="small" class="mt-3" @click="form.email.push('')">
              <lf-icon name="plus" />
              Add another email
            </lf-button>
          </article>

          <article>
            <lf-field label-text="Identities">
              <div class="flex flex-col gap-3">
                <div v-for="(identity) of form.identities" :key="identity.platform">
                  <lf-input v-model="identity.value" :placeholder="`${lfIdentities[identity.platform]?.member?.placeholder || ''}...`" class="h-10">
                    <template #prefix>
                      <div class="flex items-center flex-nowrap whitespace-nowrap">
                        <div class="min-w-5">
                          <lf-tooltip :content="identity.platform">
                            <img :src="lfIdentities[identity.platform]?.image" class="h-5 min-w-5" :alt="identity.platform" />
                          </lf-tooltip>
                        </div>
                        <p
                          v-if="lfIdentities[identity.platform]?.member?.urlPrefix"
                          class="-mr-2 pl-2"
                          :class="identity.value?.length ? 'text-black' : 'text-gray-400'"
                        >
                          {{ lfIdentities[identity.platform]?.member?.urlPrefix }}
                        </p>
                      </div>
                    </template>
                  </lf-input>
                </div>
              </div>
            </lf-field>
          </article>
        </div>
      </lf-scroll-shadow>
    </section>

    <section class="border-t border-gray-100 py-4 px-6 gap-4 flex justify-end  z-40">
      <lf-button type="secondary-ghost" @click="isModalOpen = false">
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        :disabled="$v.$invalid"
        :loading="sending"
        @click="createContributor"
      >
        Add person
      </lf-button>
    </section>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import {
  computed, reactive, ref,
} from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfField from '@/ui-kit/field/Field.vue';
import { Contributor, ContributorIdentity } from '@/modules/contributor/types/Contributor';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import LfContributorAddEmailItem from '@/modules/contributor/components/edit/add/contributor-add-email-item.vue';
import LfScrollShadow from '@/ui-kit/scrollshadow/ScrollShadow.vue';
import { ContributorApiService } from '@/modules/contributor/services/contributor.api.service';
import { useRouter } from 'vue-router';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

import { ToastStore } from '@/shared/message/notification';
import Errors from '@/shared/error/errors';
import AppLfSubProjectsListDropdown from '@/modules/admin/modules/projects/components/lf-sub-projects-list-dropdown.vue';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';
import { IdentityConfig, lfIdentities } from '@/config/identities';

const props = defineProps<{
  modelValue: boolean,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void }>();

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value) => emit('update:modelValue', value),
});

const router = useRouter();

const { memberIdentities } = useIdentitiesHelpers();

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

type ContributorAddIdentity = ContributorIdentity & {
  // image: string,
  // prefix: string,
  // placeholder: string,
}

const platformList: ContributorAddIdentity[] = memberIdentities
  .map((config: IdentityConfig) => ({
    platform: config.key,
    type: 'username',
    value: '',
    verified: true,
    source: 'ui',
    sourceId: null,
  }));

interface ContributorAddForm {
  subproject: string;
  name: string;
  email: string[];
  identities: ContributorAddIdentity[];
}

const form = reactive<ContributorAddForm>({
  subproject: '',
  name: '',
  email: [''],
  identities: platformList,
});

const rules = {
  subproject: {
    required,
  },
  name: {
    required,
  },
  email: {
    required: (value: ContributorAddIdentity[]) => value.some((i) => i.length),
  },
  identities: {
    required: (value: ContributorAddIdentity[]) => value.some((i) => i.value.length),
  },
};

const $v = useVuelidate(rules, form);

const resetForm = () => {
  form.subproject = '';
  form.name = '';
  form.email = [''];
  form.identities = platformList;
  $v.value.$reset();
};

const sending = ref<boolean>(false);
const createContributor = () => {
  if ($v.value.$invalid) {
    return;
  }

  const data: Partial<Contributor> = {
    displayName: form.name,
    identities: [
      ...form.identities
        .filter((i) => !!i.value)
        .map((i) => ({
          type: 'username',
          platform: i.platform,
          value: i.value,
          verified: true,
          source: 'ui',
          sourceId: null,
        })),
      ...form.email
        .filter((email) => !!email)
        .map((email) => ({
          type: 'email',
          platform: 'custom',
          value: email,
          verified: true,
          source: 'ui',
          sourceId: null,
        })),
    ],
    username: {},
    manuallyCreated: true,
  };

  sending.value = true;

  ContributorApiService.create(data, [form.subproject])
    .then((newMember) => {
      router.push({
        name: 'memberView',
        params: {
          id: newMember.id,
        },
        query: {
          projectGroup: selectedProjectGroup.value?.id,
        },
      });
      isModalOpen.value = false;
      resetForm();
    })
    .catch((error) => {
      if (error.response.status === 409) {
        ToastStore.error('Person was not created because the identity already exists in another profile');
      } else {
        Errors.handle(error);
      }
    })
    .finally(() => {
      sending.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfContributorAdd',
};
</script>
