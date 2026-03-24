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
          <app-member-suggestions-details
            v-if="!preview && props.modelValue"
            :member="props.modelValue"
            :is-primary="true"
          >
            <template #header>
              <div class="h-13 flex justify-between items-start">
                <div
                  class="bg-primary-500 rounded-full py-0.5 px-2 text-white inline-block text-xs leading-5 font-medium"
                >
                  Current profile
                </div>
              </div>
            </template>
            <template #property>
              <article v-if="props.modelValue.activityCount" class="pb-4">
                <p class="text-2xs font-medium text-gray-500 pb-1">
                  Activity count
                </p>
                <p class="text-xs text-gray-900 whitespace-normal">
                  {{ props.modelValue.activityCount || '-' }}
                </p>
              </article>
            </template>
            <template #below>
              <div v-if="props.modelValue?.organizations?.length" class="pt-8">
                <h6 class="text-sm font-semibold text-black pb-4">
                  Organizations
                </h6>
                <app-member-organization-list :member="props.modelValue" />
              </div>
            </template>
          </app-member-suggestions-details>
          <app-member-suggestions-details
            v-else-if="preview"
            :member="preview.primary"
            :compare-member="preview.secondary"
            :is-primary="true"
          >
            <template #header>
              <div class="h-13 flex justify-between items-start">
                <div
                  class="bg-primary-500 rounded-full py-0.5 px-2 text-white inline-block text-xs leading-5 font-medium"
                >
                  Updated profile
                </div>
              </div>
            </template>
            <template #property>
              <article
                v-if="
                  preview.primary.activityCount
                    || preview.secondary.activityCount
                "
                class="pb-4"
              >
                <p class="text-2xs font-medium text-gray-500 pb-1">
                  Activity count
                </p>
                <p class="text-xs text-gray-900 whitespace-normal">
                  {{ preview.primary.activityCount || '-' }}
                </p>
              </article>
            </template>
            <template #engagementLevel>
              <div class="flex items-center">
                <div
                  class="border border-gray-200 bg-gray-100 py-px px-1.5 text-gray-600 text-xs leading-5 rounded-md mr-1"
                >
                  Unknown
                </div>
                <el-tooltip
                  content="Calculated after profile is unmerged"
                  placement="top"
                >
                  <lf-icon
                    name="circle-question"
                    :size="16"
                    class="text-gray-400"
                  />
                </el-tooltip>
              </div>
            </template>
            <template #below>
              <div v-if="preview.primary?.organizations?.length" class="pt-8">
                <h6 class="text-sm font-semibold text-black pb-4">
                  Organizations
                </h6>
                <app-member-organization-list :member="preview.primary" />
              </div>
            </template>
          </app-member-suggestions-details>
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
            <app-member-suggestions-details
              :member="preview.secondary"
              :compare-member="props.modelValue"
            >
              <template #header>
                <div class="h-13">
                  <div class="flex justify-between items-start">
                    <div
                      class="inline-flex items-center bg-gray-100 rounded-full py-0.5 px-2 text-gray-600 text-xs leading-5 font-medium"
                    >
                      <lf-icon
                        name="link-simple-slash"
                        :size="16"
                        class="mr-1"
                      />
                      Unmerged profile
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
                            v-if="i.id !== localSelectedIdentity"
                            :value="i.id"
                            :label="i.value"
                            @click="changeIdentity(i.id)"
                          >
                            <lf-icon
                              v-if="i.type === 'email'"
                              name="envelope"
                              :size="20"
                              class="text-gray-900 leading-5 mr-2"
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
                            <span>{{ i.value }}</span>
                          </el-dropdown-item>
                        </template>
                      </template>
                    </el-dropdown>
                  </div>
                </div>
              </template>
              <template #engagementLevel>
                <div class="flex items-center">
                  <div
                    class="border border-gray-200 bg-gray-100 py-px px-1.5 text-gray-600 text-xs leading-5 rounded-md mr-1"
                  >
                    Unknown
                  </div>
                  <el-tooltip
                    content="Calculated after profile is unmerged"
                    placement="top"
                  >
                    <lf-icon
                      name="circle-question"
                      :size="16"
                      class="text-gray-400"
                    />
                  </el-tooltip>
                </div>
              </template>
              <template #property>
                <article
                  v-if="
                    preview.primary.activityCount
                      || preview.secondary.activityCount
                  "
                  class="pb-4"
                >
                  <p class="text-2xs font-medium text-gray-500 pb-1">
                    Activity count
                  </p>
                  <p class="text-xs text-gray-900 whitespace-normal">
                    {{ preview.secondary.activityCount || '-' }}
                  </p>
                </article>
              </template>
              <template #below>
                <div
                  v-if="preview.secondary?.organizations?.length"
                  class="pt-8"
                >
                  <h6 class="text-sm font-semibold text-black pb-4">
                    Organizations
                  </h6>
                  <app-member-organization-list :member="preview.secondary" />
                </div>
              </template>
            </app-member-suggestions-details>
          </div>
          <!-- Identity selection -->
          <div v-else class="pt-14">
            <div class="flex justify-center pb-5">
              <lf-icon name="fingerprint" :size="64" class="text-gray-200" />
            </div>
            <p class="text-center text-xs leading-5 text-gray-500">
              Select the profile identity you want to unmerge
            </p>
            <div class="pt-4">
              <el-select
                placeholder="Select identity"
                class="w-full"
                filterable
                @update:model-value="changeIdentity($event)"
              >
                <el-option
                  v-for="i of identities"
                  :key="i.id"
                  :value="i.id"
                  :label="i.value"
                >
                  <lf-icon
                    v-if="i.type === 'email'"
                    name="envelope"
                    :size="20"
                    class="text-gray-900 leading-5 mr-2"
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
                  {{ i.value }}
                </el-option>
              </el-select>
            </div>
          </div>
        </div>
      </div>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';

import { MemberService } from '@/modules/member/member-service';

import { ToastStore } from '@/shared/message/notification';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import AppMemberOrganizationList from '@/modules/member/components/suggestions/member-organizations-list.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import { useRouter } from 'vue-router';
import { lfIdentities } from '@/config/identities';
import LfButton from '@/ui-kit/button/Button.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import { Contributor } from '@/modules/contributor/types/Contributor';
import LfModal from '@/ui-kit/modal/Modal.vue';
import AppMemberSuggestionsDetails from './suggestions/member-merge-suggestions-details.vue';

const props = defineProps<{
  modelValue: Contributor;
  selectedIdentity?: string | null;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: Contributor | null): void;
}>();

const router = useRouter();

const { trackEvent } = useProductTracking();

const { getContributor } = useContributorStore();

const { getContributorMergeActions } = useContributorStore();

const unmerging = ref(false);
const canRevertPreviousMerge = ref(false);
const revertPreviousMerge = ref(false);
const fetchingPreview = ref(false);
const preview = ref<{
  primary: Contributor;
  secondary: Contributor;
} | null>(null);
const localSelectedIdentity = ref<string | null>(null);

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

const identityOrder = ['username', 'email'];

const identities = computed(() => (props.modelValue.identities || []).sort((a, b) => {
  const aIndex = identityOrder.indexOf(a.type);
  const bIndex = identityOrder.indexOf(b.type);
  const aOrder = aIndex !== -1 ? aIndex : identityOrder.length;
  const bOrder = bIndex !== -1 ? bIndex : identityOrder.length;
  return aOrder - bOrder;
}));

const changeIdentity = (identityId: string) => {
  localSelectedIdentity.value = identityId;
  resetRevertPreviousMerge();
  getCanRevertMerge(identityId);
  fetchPreview(identityId);
};

const fetchPreview = (identityId: string) => {
  if (fetchingPreview.value) {
    return;
  }

  fetchingPreview.value = true;

  MemberService.unmergePreview(
    props.modelValue?.id,
    identityId,
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

const getCanRevertMerge = (identityId: string) => {
  MemberService.canRevertMerge(props.modelValue?.id, identityId)
    .then((res) => {
      canRevertPreviousMerge.value = res;
    })
    .catch(() => {
      canRevertPreviousMerge.value = false;
    });
};

const unmerge = () => {
  if (unmerging.value) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.UNMERGE_MEMBER_IDENTITY,
    type: EventType.FEATURE,
    properties: {
      identity: localSelectedIdentity.value,
    },
  });

  unmerging.value = true;

  MemberService.unmerge(props.modelValue?.id, preview.value)
    .then(() => {
      getContributorMergeActions(props.modelValue?.id);
      ToastStore.info(
        "We're finalizing profiles unmerging. We will let you know once the process is completed.",
        {
          title: 'Profiles unmerging in progress',
        },
      );
      getContributor(props.modelValue?.id).then(() => {
        router.replace({
          params: {
            id: props.modelValue?.id,
          },
        });
      });
      emit('update:modelValue', null);
    })
    .catch((error) => {
      ToastStore.error('There was an error unmerging profile');
    })
    .finally(() => {
      unmerging.value = false;
    });
};

onMounted(() => {
  if (props.selectedIdentity) {
    changeIdentity(props.selectedIdentity);
  }
});
</script>

<script lang="ts">
export default {
  name: 'AppMemberUnmergeDialog',
};
</script>
