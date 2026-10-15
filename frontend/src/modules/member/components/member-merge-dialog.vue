<template>
  <lf-modal
    v-if="isModalOpen"
    v-model="isModalOpen"
    header-title="Merge profile"
    container-class="overflow-auto"
    content-class="!max-h-none"
    width="64rem"
  >
    <div class="p-6 relative border-t">
      <div class="flex -mx-3">
        <div class="w-1/2 px-3">
          <app-member-suggestions-details
            v-if="props.modelValue"
            :member="props.modelValue"
            :compare-member="memberToMerge"
            :is-primary="originalMemberPrimary"
            @make-primary="originalMemberPrimary = true"
          />
        </div>
        <div class="w-1/2 px-3">
          <app-member-selection-dropdown
            v-if="!memberToMerge"
            :id="props.modelValue?.id"
            v-model="memberToMerge"
            style="margin-right: 5px"
          />
          <app-member-suggestions-details
            v-else
            :member="memberToMerge"
            :compare-member="props.modelValue"
            :is-primary="!originalMemberPrimary"
            @make-primary="originalMemberPrimary = false"
          >
            <template #action>
              <button
                class="btn btn-link btn-link--sm btn-link--primary leading-5 !px-4 !py-1"
                type="button"
                @click="changeMember()"
              >
                <lf-icon
                  name="arrows-rotate"
                  :size="16"
                  class="text-primary-500 mr-2"
                />
                <span class="text-primary-500">Change person</span>
              </button>
            </template>
          </app-member-suggestions-details>
        </div>
      </div>
      <div class="pt-6 flex justify-end">
        <lf-button
          type="secondary-gray"
          size="large"
          class="mr-4"
          @click="emit('update:modelValue', null)"
        >
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          size="large"
          :disabled="!memberToMerge"
          :loading="sendingMerge"
          @click="mergeSuggestion()"
        >
          Merge profile
        </lf-button>
      </div>
    </div>
  </lf-modal>
</template>

<script setup>
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { MemberService } from '@/modules/member/member-service';

import { ToastStore } from '@/shared/message/notification';
import { useMemberStore } from '@/modules/member/store/pinia';
import useMemberMergeMessage from '@/shared/modules/merge/config/useMemberMergeMessage';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import AppMemberSelectionDropdown from './member-selection-dropdown.vue';
import AppMemberSuggestionsDetails from './suggestions/member-merge-suggestions-details.vue';

const props = defineProps({
  modelValue: {
    type: Object,
    required: true,
  },
  toMerge: {
    type: Object,
    default: null,
  },
});

const emit = defineEmits(['update:modelValue']);

const { trackEvent } = useProductTracking();

const route = useRoute();
const router = useRouter();

const memberStore = useMemberStore();

const originalMemberPrimary = ref(true);
const sendingMerge = ref(false);

const memberToMerge = ref(null);

const isModalOpen = computed({
  get() {
    return props.modelValue !== null;
  },
  set() {
    emit('update:modelValue', null);
    memberToMerge.value = null;
  },
});

watch(
  () => props.toMerge,
  (toMerge) => {
    if (toMerge) {
      memberToMerge.value = toMerge;
    }
  },
  {
    immediate: true,
  },
);

const changeMember = () => {
  memberToMerge.value = null;
  originalMemberPrimary.value = true;
};

const mergeSuggestion = () => {
  if (sendingMerge.value) {
    return;
  }

  sendingMerge.value = true;

  const primaryMember = originalMemberPrimary.value
    ? props.modelValue
    : memberToMerge.value;
  const secondaryMember = originalMemberPrimary.value
    ? memberToMerge.value
    : props.modelValue;

  const { loadingMessage, apiErrorMessage } = useMemberMergeMessage;

  loadingMessage();

  trackEvent({
    key: FeatureEventKey.MERGE_MEMBER,
    type: EventType.FEATURE,
    properties: {
      path: route.path,
    },
  });

  MemberService.merge(primaryMember, secondaryMember)
    .then(() => {
      ToastStore.closeAll();
      ToastStore.info(
        "We're finalizing profiles merging. We will let you know once the process is completed.",
        {
          title: 'Profiles merging in progress',
        },
      );
      emit('update:modelValue', null);

      if (route.name === 'memberView') {
        memberStore.findMember(primaryMember.id).then(() => {
          router.replace({
            params: {
              id: primaryMember.id,
            },
          });
        });
      }
      emit('update:modelValue', null);
    })
    .catch((error) => {
      apiErrorMessage({ error });
    })
    .finally(() => {
      sendingMerge.value = false;
    });
};
</script>

<script>
export default {
  name: 'AppMemberMergeDialog',
};
</script>
