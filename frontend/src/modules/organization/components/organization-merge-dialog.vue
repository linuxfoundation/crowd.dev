<template>
  <lf-modal
    v-if="isModalOpen"
    v-model="isModalOpen"
    header-title="Merge organization"
    container-class="overflow-auto"
    content-class="!max-h-none"
    width="64rem"
  >
    <div class="p-6 relative border-t">
      <div class="flex -mx-3">
        <div class="w-1/2 px-3">
          <app-organization-merge-suggestions-details
            v-if="props.modelValue"
            :organization="props.modelValue"
            :compare-organization="organizationToMerge"
            :is-primary="originalOrganizationPrimary"
            @make-primary="originalOrganizationPrimary = true"
          />
        </div>
        <div class="w-1/2 px-3">
          <app-organization-selection-dropdown
            v-if="!organizationToMerge"
            :id="props.modelValue?.id"
            v-model="organizationToMerge"
            :primary-organization="props.modelValue"
            style="margin-right: 5px"
            @update:model-value="checkPrimaryOrganization"
          />
          <app-organization-merge-suggestions-details
            v-else
            :organization="organizationToMerge"
            :compare-organization="props.modelValue"
            :is-primary="!originalOrganizationPrimary"
            @make-primary="originalOrganizationPrimary = false"
          >
            <template #action>
              <button
                class="btn btn--transparent btn--sm leading-5 !px-4 !py-1"
                type="button"
                @click="changeOrganization()"
              >
                <lf-icon
                  name="arrows-rotate"
                  :size="16"
                  class="text-primary-500 mr-2"
                />
                <span class="text-primary-500">Change organization</span>
              </button>
            </template>
          </app-organization-merge-suggestions-details>
        </div>
      </div>
      <div class="pt-6 flex justify-end">
        <lf-button
          type="bordered"
          size="large"
          class="mr-4"
          @click="emit('update:modelValue', null)"
        >
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          size="large"
          :disabled="!organizationToMerge"
          :loading="sendingMerge"
          @click="mergeSuggestion()"
        >
          Merge organizations
        </lf-button>
      </div>
    </div>
  </lf-modal>
</template>

<script setup>
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import AppOrganizationMergeSuggestionsDetails from '@/modules/organization/components/suggestions/organization-merge-suggestions-details.vue';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { useInvalidate } from '@/shared/queries/invalidate';
import { OrganizationService } from '@/modules/organization/organization-service';
import AppOrganizationSelectionDropdown from '@/modules/organization/components/organization-selection-dropdown.vue';
import useOrganizationMergeMessage from '@/shared/modules/merge/config/useOrganizationMergeMessage';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';

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

const organizationStore = useOrganizationStore();
const invalidate = useInvalidate();

const originalOrganizationPrimary = ref(true);
const sendingMerge = ref(false);

const organizationToMerge = ref(null);

const isModalOpen = computed({
  get() {
    return props.modelValue !== null;
  },
  set() {
    emit('update:modelValue', null);
    organizationToMerge.value = null;
  },
});

watch(
  () => props.toMerge,
  (toMerge) => {
    if (toMerge) {
      organizationToMerge.value = toMerge;
    }
  },
);

const changeOrganization = () => {
  organizationToMerge.value = null;
  originalOrganizationPrimary.value = true;
};

const checkPrimaryOrganization = (orgToMerge) => {
  if (orgToMerge.lfxMembership) {
    originalOrganizationPrimary.value = false;
  }
};

const mergeSuggestion = () => {
  if (sendingMerge.value) {
    return;
  }

  sendingMerge.value = true;

  const primaryOrganization = originalOrganizationPrimary.value
    ? props.modelValue
    : organizationToMerge.value;
  const secondaryOrganization = originalOrganizationPrimary.value
    ? organizationToMerge.value
    : props.modelValue;

  const { loadingMessage, apiErrorMessage } = useOrganizationMergeMessage;

  trackEvent({
    key: FeatureEventKey.MERGE_ORGANIZATION,
    type: EventType.FEATURE,
    properties: {
      path: route.path,
    },
  });

  OrganizationService.mergeOrganizations(
    primaryOrganization.id,
    secondaryOrganization.id,
  )
    .then(() => {
      organizationStore.addMergedOrganizations(
        primaryOrganization.id,
        secondaryOrganization.id,
      );

      loadingMessage();

      emit('update:modelValue', null);

      if (route.name === 'organizationView') {
        const segments = route.query.segmentId
          ? [route.query.segmentId]
          : [route.query.projectGroup];

        organizationStore
          .fetchOrganization(primaryOrganization.id, segments)
          .then((res) => {
            router.replace({
              params: {
                id: res.id,
              },
            });
          });
      } else if (route.name === 'organization') {
        organizationStore.selectedOrganizations = [];
        invalidate.organizations();
      }

      changeOrganization();
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
  name: 'AppOrganizationMergeDialog',
};
</script>
