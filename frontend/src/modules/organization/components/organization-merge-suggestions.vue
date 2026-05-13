<template>
  <div class="panel !p-0">
    <!-- Header -->
    <header class="flex items-center justify-between px-6 py-5 border-b">
      <div class="flex items-center gap-4">
        <div class="flex items-center gap-2">
          <lf-button
            type="secondary"
            size="small"
            :disabled="loading || currentOffset <= 0 || count === 0"
            :icon-only="true"
            @click="fetch(currentOffset - 1)"
          >
            <lf-icon name="chevron-left" />
          </lf-button>
          <lf-button
            type="secondary"
            size="small"
            :disabled="loading || currentOffset >= count - 1 || count === 0"
            :icon-only="true"
            @click="fetch(currentOffset + 1)"
          >
            <lf-icon name="chevron-right" />
          </lf-button>
        </div>

        <app-loading v-if="loading" height="16px" width="128px" radius="3px" />
        <div
          v-else-if="Math.ceil(count) > 1"
          class="text-xs leading-5 text-gray-500"
        >
          <div>{{ currentOffset + 1 }} of {{ Math.ceil(count) }} suggestions</div>
        </div>
        <div
          v-else-if="Math.ceil(count) === 1"
          class="text-xs leading-5 text-gray-500"
        >
          <div>1 suggestion</div>
        </div>
        <div
          v-else
          class="text-xs leading-5 text-gray-500"
        >
          <div>0 suggestions</div>
        </div>
      </div>
      <div class="flex items-center gap-4">
        <app-member-merge-similarity v-if="!loading && organizationsToMerge.similarity" :similarity="organizationsToMerge.similarity" />
        <lf-button
          type="secondary"
          :disabled="loading || isEditLockedForSampleData || count === 0"
          :loading="sendingIgnore"
          @click="ignoreSuggestion()"
        >
          Ignore suggestion
        </lf-button>
        <lf-button
          type="primary"
          :disabled="loading || isEditLockedForSampleData || count === 0"
          :loading="sendingMerge"
          @click="mergeSuggestion()"
        >
          Merge organizations
        </lf-button>
        <slot name="actions" />
      </div>
    </header>

    <div v-if="loading || count > 0">
      <!-- Comparison -->
      <!-- Loading -->
      <div v-if="loading" class="flex p-5">
        <div class="w-1/3 rounded-lg">
          <app-organization-merge-suggestions-details
            :organization="null"
            :loading="true"
            :is-primary="true"
          />
        </div>
        <div class="w-1/3 -ml-px rounded-lg">
          <app-organization-merge-suggestions-details
            :organization="null"
            :loading="true"
          />
        </div>

        <div class="w-1/3 ml-8 rounded-lg bg-primary-25">
          <app-member-merge-suggestions-details
            :member="null"
            :loading="true"
          />
        </div>
      </div>
      <div v-else class="flex p-5">
        <div
          v-for="(organization, mi) of organizationsToMerge.organizations"
          :key="organization.id"
          class="w-1/3"
        >
          <app-organization-merge-suggestions-details
            :organization="organization"
            :compare-organization="
              organizationsToMerge.organizations[(mi + 1) % organizationsToMerge.organizations.length]
            "
            :is-primary="mi === primary"
            :extend-bio="bioHeight"
            class="rounded-lg"
            :class="mi > 0 ? '-ml-px' : ''"
            @make-primary="primary = mi"
            @bio-height="$event > bioHeight ? (bioHeight = $event) : null"
          />
        </div>
        <div class="w-1/3 ml-8">
          <app-organization-merge-suggestions-details
            :organization="preview"
            :is-preview="true"
            class="rounded-lg bg-primary-25"
          />
        </div>
      </div>

    <!-- Actions -->
    </div>
    <!-- Empty state -->
    <div v-else class="py-20 flex flex-col items-center pb-20">
      <lf-icon name="shuffle" :size="160" class="text-gray-200 flex items-center mb-8" />
      <h5 class="text-center text-lg font-semibold mb-4">
        No merge suggestions
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn’t find any duplicated organizations
      </p>
    </div>
  </div>
</template>

<script setup>
import {
  ref, onMounted, computed, onUnmounted,
} from 'vue';
import { ToastStore } from '@/shared/message/notification';
import AppLoading from '@/shared/loading/loading-placeholder.vue';
import AppOrganizationMergeSuggestionsDetails from '@/modules/organization/components/suggestions/organization-merge-suggestions-details.vue';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { merge } from 'lodash';
import AppMemberMergeSuggestionsDetails
  from '@/modules/member/components/suggestions/member-merge-suggestions-details.vue';
import useOrganizationMergeMessage from '@/shared/modules/merge/config/useOrganizationMergeMessage';
import LfButton from '@/ui-kit/button/Button.vue';
import AppMemberMergeSimilarity from '@/modules/member/components/suggestions/member-merge-similarity.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { OrganizationService } from '../organization-service';

const props = defineProps({
  query: {
    type: Object,
    required: false,
    default: () => ({}),
  },
  offset: {
    type: Number,
    required: false,
    default: 0,
  },
});

const emit = defineEmits(['reload']);

const organizationStore = useOrganizationStore();

const { trackEvent } = useProductTracking();

const organizationsToMerge = ref([]);
const primary = ref(0);
const currentOffset = ref(0);
const count = ref(0);
const loading = ref(false);

const sendingIgnore = ref(false);
const sendingMerge = ref(false);

const changed = ref(false);

const bioHeight = ref(0);

const clearOrganization = (organization) => {
  const cleanedOrganization = { ...organization };
  // eslint-disable-next-line no-restricted-syntax
  for (const key in cleanedOrganization) {
    if (!cleanedOrganization[key]) {
      delete cleanedOrganization[key];
    }
  }
  return cleanedOrganization;
};

const preview = computed(() => {
  const primaryOrganization = organizationsToMerge.value.organizations[primary.value];
  const secondaryOrganization = organizationsToMerge.value.organizations[(primary.value + 1) % 2];
  const mergedOrganizations = merge({}, clearOrganization(secondaryOrganization), clearOrganization(primaryOrganization));
  if (!Array.isArray(primaryOrganization.identities)) {
    primaryOrganization.identities = [];
  }
  if (!Array.isArray(secondaryOrganization.identities)) {
    secondaryOrganization.identities = [];
  }

  mergedOrganizations.identities = [...(primaryOrganization.identities || []), ...(secondaryOrganization.identities || [])];
  return mergedOrganizations;
});

const fetch = (page) => {
  if (page > -1) {
    currentOffset.value = page;
  }

  trackEvent({
    key: FeatureEventKey.NAVIGATE_ORGANIZATIONS_MERGE_SUGGESTIONS,
    type: EventType.FEATURE,
  });

  loading.value = true;

  OrganizationService.fetchMergeSuggestions(1, currentOffset.value, props.query ?? {})
    .then((res) => {
      currentOffset.value = +res.offset;
      count.value = res.count;
      [organizationsToMerge.value] = res.rows;

      primary.value = 0;
    })
    .catch(() => {
      ToastStore.error(
        'There was an error fetching merge suggestion, please try again later',
      );
    })
    .finally(() => {
      loading.value = false;
    });
};

const ignoreSuggestion = () => {
  if (sendingIgnore.value || sendingMerge.value || loading.value) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.IGNORE_ORGANIZATION_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: organizationsToMerge.value.similarity,
    },
  });

  sendingIgnore.value = true;
  OrganizationService.addToNoMerge(...organizationsToMerge.value.organizations)
    .then(() => {
      ToastStore.success('Merging suggestion ignored successfully');

      const nextIndex = currentOffset.value >= (count.value - 1) ? Math.max(count.value - 2, 0) : currentOffset.value;
      fetch(nextIndex);
      changed.value = true;
    })
    .catch((error) => {
      if (error.response.status === 404) {
        ToastStore.error('Suggestion already merged or ignored', {
          message: `Sorry, the suggestion you are trying to merge might have already been merged or ignored.
          Please refresh to see the updated information.`,
        });
      } else {
        ToastStore.error('There was an error ignoring the merging suggestion');
      }
    })
    .finally(() => {
      sendingIgnore.value = false;
    });
};

const mergeSuggestion = () => {
  if (sendingIgnore.value || sendingMerge.value || loading.value) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.MERGE_ORGANIZATION_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: organizationsToMerge.value.similarity,
    },
  });

  sendingMerge.value = true;

  const primaryOrganization = organizationsToMerge.value.organizations[primary.value];
  const secondaryOrganization = organizationsToMerge.value.organizations[(primary.value + 1) % 2];

  const { loadingMessage, apiErrorMessage } = useOrganizationMergeMessage;

  OrganizationService.mergeOrganizations(primaryOrganization.id, secondaryOrganization.id)
    .then(() => {
      organizationStore
        .addMergedOrganizations(primaryOrganization.id, secondaryOrganization.id);

      primary.value = 0;

      loadingMessage();

      const nextIndex = currentOffset.value >= (count.value - 1) ? Math.max(count.value - 2, 0) : currentOffset.value;
      fetch(nextIndex);
      changed.value = true;
    })
    .catch((error) => {
      const shouldLoadNextSuggestion = apiErrorMessage({ error });

      if (shouldLoadNextSuggestion) {
        fetch();
      }
    })
    .finally(() => {
      sendingMerge.value = false;
    });
};

onMounted(async () => {
  fetch(props.offset);
});

onUnmounted(() => {
  if (changed.value) {
    emit('reload');
  }
});
</script>

<script>
export default {
  name: 'AppOrganizationMergeSuggestionsPage',
};
</script>
