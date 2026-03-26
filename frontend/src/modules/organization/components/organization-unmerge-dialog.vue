<template>
  <lf-modal
    v-if="isModalOpen"
    v-model="isModalOpen"
    container-class="overflow-auto"
    content-class="!max-h-none"
    width="64rem"
  >
    <div class="flex items-center justify-between p-6">
      <h3 class="text-lg font-semibold">
        Unmerge identity
      </h3>
      <div class="flex justify-end -my-1">
        <lf-button
          type="bordered"
          size="medium"
          class="mr-4"
          @click="emit('update:modelValue', null)"
        >
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          size="medium"
          :disabled="!localSelectedIdentity || !preview"
          :loading="unmerging"
          @click="unmerge()"
        >
          {{
            !revertPreviousMerge
              ? 'Unmerge identity'
              : 'Unmerge identity and revert previous merge'
          }}
        </lf-button>
      </div>
    </div>
    <div class="p-6 relative border-t">
      <div
        v-if="canRevertPreviousMerge"
        class="flex bg-yellow-50 rounded-lg w-full p-3 mb-5"
      >
        <lf-icon
          name="exclamation-triangle"
          type="solid"
          class="text-yellow-500"
          :size="18"
        />
        <div class="flex flex-col ml-2 flex-grow">
          <div class="flex items-center justify-between">
            <span class="text-black text-xs font-semibold">Do you want to revert the previous merge operation?</span>
            <lf-switch
              v-model="revertPreviousMerge"
              class="text-gray-900 text-xs"
              size="tiny"
              @update:model-value="fetchPreview(localSelectedIdentity!)"
            >
              Revert previous merge
            </lf-switch>
          </div>
          <span class="text-gray-500 text-tiny mt-1 whitespace-pre-line">
            The identity you are unmerging was part of a previous merge
            operation.
            <br />
            By reverting the previous operation, we will split the profiles
            previously merged.
          </span>
        </div>
      </div>
      <div class="flex -mx-3">
        <div class="w-1/2 px-3">
          <!-- Loading preview -->
          <app-organization-merge-suggestions-details
            v-if="!preview && props.modelValue"
            :organization="props.modelValue"
            :is-primary="true"
          >
            <template #header>
              <div class="h-13 flex justify-between items-start">
                <div
                  class="bg-primary-500 rounded-full py-0.5 px-2 text-white inline-block text-xs leading-5 font-medium"
                >
                  Current organization
                </div>
              </div>
            </template>
          </app-organization-merge-suggestions-details>
          <app-organization-merge-suggestions-details
            v-else-if="preview"
            :organization="preview.primary"
            :compare-organization="preview.secondary"
            :is-primary="true"
            :is-preview="true"
          >
            <template #header>
              <div class="h-13 flex justify-between items-start">
                <div
                  class="bg-primary-500 rounded-full py-0.5 px-2 text-white inline-block text-xs leading-5 font-medium"
                >
                  Updated organization
                </div>
              </div>
            </template>
          </app-organization-merge-suggestions-details>
        </div>
        <div class="w-1/2 px-3">
          <!-- Loading preview -->
          <div
            v-if="fetchingPreview"
            class="flex items-center justify-center pt-40 w-full"
          >
            <lf-spinner />
          </div>
          <!-- Unmerge preview -->
          <div v-else-if="preview">
            <app-organization-merge-suggestions-details
              :organization="preview.secondary"
              :compare-organization="preview.primary"
              :is-preview="true"
            >
              <template #header>
                <div class="h-13">
                  <div class="flex justify-between items-start">
                    <div
                      class="inline-flex items-center bg-gray-100 rounded-full py-0.5 px-2 text-gray-600 text-xs leading-5 font-medium"
                    >
                      <lf-icon name="link-slash" :size="16" class="mr-1" />
                      Unmerged organization
                    </div>
                    <el-dropdown placement="bottom-end" trigger="click">
                      <button
                        class="btn btn--link !text-primary-500"
                        type="button"
                        @click.stop
                      >
                        Change identity
                      </button>
                      <template #dropdown>
                        <template v-for="i of identities" :key="i.id">
                          <el-dropdown-item
                            v-if="i.id !== localSelectedIdentity!.id"
                            :value="i"
                            :label="i.displayValue"
                            @click="changeIdentity(i)"
                          >
                            <lf-icon
                              v-if="i.type === 'email'"
                              name="envelope"
                              :size="20"
                              class="text-gray-900 leading-5 mr-2"
                            />
                            <lf-icon
                              v-else-if="
                                [
                                  'primary-domain',
                                  'alternative-domain',
                                  'affiliated-profile',
                                ].includes(i.type)
                              "
                              name="window"
                              :size="20"
                              class="text-gray-900 text-lg leading-5 mr-2"
                            />
                            <img
                              v-else-if="lfIdentities[i.platform]"
                              class="h-5 min-w-5 mr-2"
                              :alt="lfIdentities[i.platform]?.name"
                              :src="lfIdentities[i.platform]?.image"
                            />
                            <lf-icon
                              v-else
                              name="fingerprint"
                              :size="20"
                              class="text-gray-600 mr-2"
                            />
                            <span>{{ i.displayValue }}</span>
                          </el-dropdown-item>
                        </template>
                      </template>
                    </el-dropdown>
                  </div>
                </div>
              </template>
            </app-organization-merge-suggestions-details>
          </div>
          <!-- Identity selection -->
          <div v-else class="pt-14">
            <div class="flex justify-center pb-5">
              <lf-icon name="fingerprint" :size="64" class="text-gray-200" />
            </div>
            <p class="text-center text-xs leading-5 text-gray-500">
              Select the organization identity you want to unmerge
            </p>
            <div class="pt-4">
              <el-select
                placeholder="Select identity"
                class="w-full"
                filterable
                value-key="id"
                @update:model-value="changeIdentity($event)"
              >
                <el-option
                  v-for="i of identities"
                  :key="i.id"
                  :value="i"
                  :label="i.displayValue"
                >
                  <lf-icon
                    v-if="i.type === 'email'"
                    name="envelope"
                    :size="20"
                    class="text-gray-900 leading-5 mr-2"
                  />
                  <lf-icon
                    v-else-if="
                      [
                        'primary-domain',
                        'alternative-domain',
                        'affiliated-profile',
                      ].includes(i.type)
                    "
                    name="window"
                    :size="20"
                    class="text-gray-900 text-lg leading-5 mr-2"
                  />
                  <img
                    v-else-if="lfIdentities[i.platform]"
                    class="h-5 min-w-5 mr-2"
                    :alt="lfIdentities[i.platform]?.name"
                    :src="lfIdentities[i.platform]?.image"
                  />
                  <lf-icon
                    v-else
                    name="fingerprint"
                    :size="20"
                    class="text-gray-600 mr-2"
                  />
                  {{ i.displayValue }}
                </el-option>
              </el-select>
            </div>
          </div>
        </div>
      </div>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">import { computed, onMounted, ref } from 'vue';
import { ToastStore } from '@/shared/message/notification';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import { OrganizationService } from '@/modules/organization/organization-service';
import AppOrganizationMergeSuggestionsDetails from '@/modules/organization/components/suggestions/organization-merge-suggestions-details.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { lfIdentities } from '@/config/identities';
import LfButton from '@/ui-kit/button/Button.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import {
  Organization,
  OrganizationIdentity,
  OrganizationIdentityParsed,
} from '@/modules/organization/types/Organization';
import LfModal from '@/ui-kit/modal/Modal.vue';

const props = defineProps<{
  modelValue: Organization | null;
  selectedIdentity?: OrganizationIdentity | null;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: Organization | null): void;
}>();

const { trackEvent } = useProductTracking();

const canRevertPreviousMerge = ref(false);
const revertPreviousMerge = ref(false);
const unmerging = ref(false);
const fetchingPreview = ref(false);
const preview = ref<{
  primary: Organization;
  secondary: Organization;
} | null>(null);
const localSelectedIdentity = ref<OrganizationIdentityParsed | null>(null);

const { getOrganizationMergeActions, fetchOrganization } = useOrganizationStore();

const parseIdentityValues = (
  identity: OrganizationIdentity,
): OrganizationIdentityParsed => {
  const splittedIdentity = identity.value?.split(':');

  if (
    identity.platform === Platform.LINKEDIN
    && splittedIdentity.length === 2
  ) {
    return {
      ...identity,
      id: `${identity.platform}:${identity.value}:${identity.type}:${identity.verified}`,
      value: identity.value,
      displayValue: splittedIdentity[1],
    };
  }

  return {
    ...identity,
    id: `${identity.platform}:${identity.value}:${identity.type}:${identity.verified}`,
    displayValue: identity.value,
  };
};

const isModalOpen = computed({
  get() {
    return props.modelValue !== null;
  },
  set() {
    emit('update:modelValue', null);
    fetchingPreview.value = false;
    localSelectedIdentity.value = null;
    preview.value = null;
  },
});

const identityOrder = [
  'username',
  'primary-domain',
  'alternative-domain',
  'affiliated-profile',
  'email',
];

const identities = computed(() => {
  if (!props.modelValue?.identities) {
    return [];
  }
  return (props.modelValue.identities || [])
    .sort((a, b) => {
      const aIndex = identityOrder.indexOf(a.type);
      const bIndex = identityOrder.indexOf(b.type);
      const aOrder = aIndex !== -1 ? aIndex : identityOrder.length;
      const bOrder = bIndex !== -1 ? bIndex : identityOrder.length;
      return aOrder - bOrder;
    })
    .map((i) => parseIdentityValues(i));
});

const changeIdentity = (identity: OrganizationIdentityParsed) => {
  localSelectedIdentity.value = identity;
  resetRevertPreviousMerge();
  getCanRevertMerge(identity);
  fetchPreview(identity);
};

const fetchPreview = (identity: OrganizationIdentityParsed) => {
  if (fetchingPreview.value) {
    return;
  }
  fetchingPreview.value = true;

  OrganizationService.unmergePreview(
    props.modelValue?.id,
    identity,
    revertPreviousMerge.value,
  )
    .then((res) => {
      preview.value = res;
    })
    .catch((error) => {
      ToastStore.error(
        error?.response?.data || 'There was an error fetching unmerge preview',
      );
    })
    .finally(() => {
      fetchingPreview.value = false;
    });
};

const resetRevertPreviousMerge = () => {
  revertPreviousMerge.value = false;
  canRevertPreviousMerge.value = false;
};

const getCanRevertMerge = (identity: OrganizationIdentityParsed) => {
  OrganizationService.canRevertMerge(props.modelValue?.id, identity)
    .then((res) => {
      canRevertPreviousMerge.value = res;
    })
    .catch(() => {
      canRevertPreviousMerge.value = false;
    });
};

const unmerge = () => {
  if (unmerging.value || !props.modelValue) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.UNMERGE_ORGANIZATION_IDENTITY,
    type: EventType.FEATURE,
    properties: {
      identity: localSelectedIdentity.value,
    },
  });

  unmerging.value = true;

  OrganizationService.unmerge(props.modelValue.id, preview.value)
    .then(() => {
      getOrganizationMergeActions(props.modelValue!.id);
      ToastStore.info(
        'We’re syncing all activities of the unmerged organization. We will let you know once the process is completed.',
        {
          title: 'Organizations unmerging in progress',
        },
      );
      fetchOrganization(props.modelValue!.id);
      emit('update:modelValue', null);
    })
    .catch(() => {
      ToastStore.error('There was an error unmerging organization');
    })
    .finally(() => {
      unmerging.value = false;
    });
};

onMounted(() => {
  if (props.selectedIdentity) {
    const identity = parseIdentityValues(props.selectedIdentity);

    changeIdentity(identity);
  }
});
</script>

<script lang="ts">
export default {
  name: 'AppOrganizationUnmergeDialog',
};
</script>
