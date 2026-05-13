<template>
  <lf-modal v-model="isModalOpen">
    <!-- Header -->
    <section class="py-4 pr-4 pl-6 flex justify-between items-center">
      <h5>Add organization</h5>
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
      <div class="px-6 py-5">
        <!-- Organization name -->
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
        <!-- Organization name -->
        <article class="mb-5">
          <lf-field label-text="Website" description="Organization primary domain" :required="true">
            <lf-input
              v-model="form.website"
              class="h-10"
              :invalid="$v.website.$invalid && $v.website.$dirty"
              @blur="$v.website.$touch()"
              @change="$v.website.$touch()"
            />
            <lf-field-messages :validation="$v.website" :error-messages="{ required: 'This field is required' }" />
          </lf-field>
        </article>

        <article>
          <lf-field label-text="Identities">
            <div class="flex flex-col gap-3">
              <div v-for="(identity) of form.identities" :key="identity.platform">
                <lf-input
                  v-model="identity.value"
                  :placeholder="`${lfIdentities[identity.platform]?.organization?.placeholder || ''}...`"
                  class="h-10"
                >
                  <template #prefix>
                    <div class="flex items-center flex-nowrap whitespace-nowrap">
                      <div class="min-w-5">
                        <lf-tooltip :content="identity.platform">
                          <img :src="lfIdentities[identity.platform]?.image" class="h-5 min-w-5 object-contain" :alt="identity.platform" />
                        </lf-tooltip>
                      </div>
                      <p
                        v-if="lfIdentities[identity.platform]?.organization?.urlPrefix"
                        class="-mr-2 pl-2"
                        :class="identity.value?.length ? 'text-black' : 'text-gray-400'"
                      >
                        {{ lfIdentities[identity.platform]?.organization?.urlPrefix }}
                      </p>
                    </div>
                  </template>
                </lf-input>
              </div>
            </div>
          </lf-field>
        </article>
      </div>
    </section>

    <section class="border-t border-gray-100 py-4 px-6 gap-4 flex justify-end  z-40">
      <lf-button type="secondary-ghost" @click="isModalOpen = false">
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        :disabled="$v.$invalid"
        :loading="sending"
        @click="createOrganization"
      >
        Add organization
      </lf-button>
    </section>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import { computed, reactive, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfField from '@/ui-kit/field/Field.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import { useRouter } from 'vue-router';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import {
  Organization,
  OrganizationIdentity,
  OrganizationIdentityType,
} from '@/modules/organization/types/Organization';
import { Platform } from '@/shared/modules/platform/types/Platform';
import { OrganizationApiService } from '@/modules/organization/services/organization.api.service';

import { ToastStore } from '@/shared/message/notification';
import Errors from '@/shared/error/errors';
import AppLfSubProjectsListDropdown from '@/modules/admin/modules/projects/components/lf-sub-projects-list-dropdown.vue';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  modelValue: boolean,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void }>();

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value) => emit('update:modelValue', value),
});

const router = useRouter();

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
const { organizationIdentities } = useIdentitiesHelpers();

const platformList: OrganizationIdentity[] = organizationIdentities
  .map((config) => ({
    platform: config.key as Platform,
    type: OrganizationIdentityType.USERNAME,
    value: '',
    verified: true,
    source: 'ui',
    sourceId: null,
  }));

interface OrganizationAddForm {
  subproject: string;
  name: string;
  website: string;
  identities: OrganizationIdentity[];
}

const form = reactive<OrganizationAddForm>({
  subproject: '',
  name: '',
  website: '',
  identities: platformList,
});

const rules = {
  subproject: {
    required,
  },
  name: {
    required,
  },
  website: {
    required,
  },
  identities: {
    required: (value: OrganizationIdentity[]) => value.some((i) => i.value.length),
  },
};

const $v = useVuelidate(rules, form);

const resetForm = () => {
  form.subproject = '';
  form.name = '';
  form.website = '';
  form.identities = platformList;
  $v.value.$reset();
};

const sending = ref<boolean>(false);
const createOrganization = () => {
  if ($v.value.$invalid) {
    return;
  }

  const data: Partial<Organization> = {
    manuallyCreated: true,
    attributes: {
      name: {
        default: form.name,
        custom: [form.name],
      },
    },
    displayName: form.name,
    identities: [
      ...form.identities
        .filter((i) => !!i.value)
        .map((i) => ({
          type: OrganizationIdentityType.USERNAME,
          platform: i.platform as Platform,
          value: i.value,
          verified: true,
          source: 'ui',
        })),
      {
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        platform: Platform.CUSTOM,
        value: form.website,
        verified: true,
        source: 'ui',
      },
    ],
    segments: [form.subproject],
  };

  sending.value = true;

  OrganizationApiService.create(data, [form.subproject])
    .then((newOrg: Organization) => {
      router.push({
        name: 'organizationView',
        params: {
          id: newOrg.id,
        },
        query: {
          projectGroup: selectedProjectGroup.value?.id,
        },
      });
      isModalOpen.value = false;
      resetForm();
    })
    .catch((error: any) => {
      if (error.response.status === 409) {
        ToastStore.error('Organization was not created because the identity already exists in another profile');
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
  name: 'LfOrganizationAdd',
};
</script>
