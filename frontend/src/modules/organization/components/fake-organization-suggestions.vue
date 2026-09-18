<template>
  <div class="panel !p-0">
    <header class="flex items-center justify-between px-6 py-5 border-b whitespace-nowrap">
      <div class="flex items-center gap-4 shrink-0">
        <div class="flex items-center gap-2">
          <lf-button
            type="secondary"
            size="small"
            :disabled="loading || currentOffset <= 0 || !hasSuggestion"
            :icon-only="true"
            @click="fetch(currentOffset - 1)"
          >
            <lf-icon name="chevron-left" :size="16" />
          </lf-button>
          <lf-button
            type="secondary"
            size="small"
            :disabled="loading || !hasMore"
            :icon-only="true"
            @click="fetch(currentOffset + 1)"
          >
            <lf-icon name="chevron-right" :size="16" />
          </lf-button>
        </div>

        <app-loading v-if="loading" height="16px" width="128px" radius="3px" />
        <div
          v-else-if="hasSuggestion"
          class="text-xs leading-5 text-gray-500"
        >
          <div>Suggestion {{ currentOffset + 1 }}</div>
        </div>
        <div
          v-else
          class="text-xs leading-5 text-gray-500"
        >
          <div>0 suggestions</div>
        </div>
      </div>
      <div class="flex items-center gap-4 shrink-0">
        <lf-button
          type="secondary"
          :disabled="loading || !hasSuggestion"
          :loading="sendingDismiss"
          @click="dismiss()"
        >
          Dismiss suggestion
        </lf-button>
        <lf-button
          type="primary"
          :disabled="loading || !hasSuggestion"
          :loading="sendingMark"
          @click="markAsFake()"
        >
          Mark as fake
        </lf-button>
        <slot name="actions" />
      </div>
    </header>

    <div v-if="loading || hasSuggestion">
      <div class="flex p-5">
        <div class="w-full">
          <app-organization-merge-suggestions-details
            :organization="suggestion.organization"
            :loading="loading"
            :is-preview="true"
            :two-column="true"
            class="rounded-lg bg-primary-25"
          />
        </div>
      </div>
    </div>
    <div v-else class="py-20 flex flex-col items-center">
      <lf-icon name="shuffle" :size="160" class="text-gray-200 flex items-center mb-8" />
      <h5 class="text-center text-lg font-semibold mb-4">
        No fake organization suggestions
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn't find any fake organization suggestions
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import AppLoading from '@/shared/loading/loading-placeholder.vue';
import AppOrganizationMergeSuggestionsDetails from '@/modules/organization/components/suggestions/organization-merge-suggestions-details.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { ToastStore } from '@/shared/message/notification';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { OrganizationService } from '@/modules/organization/organization-service';

const props = withDefaults(
  defineProps<{
    offset?: number;
    segments?: string[];
  }>(),
  {
    offset: 0,
    segments: () => [],
  },
);

const emit = defineEmits<{(e: 'reload'): void}>();

const { trackEvent } = useProductTracking();

const suggestion = ref<any>({});
const currentOffset = ref(props.offset);
const hasMore = ref(false);
const hasSuggestion = ref(false);
const loading = ref(false);
const sendingDismiss = ref(false);
const sendingMark = ref(false);
const changed = ref(false);

const suggestionQuery = () => (props.segments?.length ? { segments: props.segments } : {});

const fetch = (page: number) => {
  if (page > -1) {
    currentOffset.value = page;
  }

  loading.value = true;

  return OrganizationService.fetchFakeOrganizationSuggestions(1, currentOffset.value, suggestionQuery())
    .then((res: any) => {
      currentOffset.value = +res.offset;
      hasMore.value = Boolean(res.hasMore);
      const [row] = res.rows || [];

      if (row?.organizationId) {
        hasSuggestion.value = true;
        suggestion.value = {
          ...row,
          organization: row.organization ?? {
            id: row.organizationId,
            displayName: row.displayName,
            logo: row.logo,
            activityCount: row.activityCount,
          },
        };
        return undefined;
      }

      hasSuggestion.value = false;
      suggestion.value = {};

      if (currentOffset.value > 0) {
        return fetch(currentOffset.value - 1);
      }

      return undefined;
    })
    .catch(() => {
      ToastStore.error(
        'There was an error fetching fake organization suggestion, please try again later',
      );
    })
    .finally(() => {
      loading.value = false;
    });
};

const markAsFake = () => {
  if (sendingDismiss.value || sendingMark.value || loading.value || !suggestion.value.organizationId) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.MARK_FAKE_ORGANIZATION_SUGGESTION,
    type: EventType.FEATURE,
  });

  sendingMark.value = true;

  OrganizationService.update(suggestion.value.organizationId, {
    isAffiliationBlocked: true,
  })
    .then(() => {
      changed.value = true;
      fetch(currentOffset.value);
    })
    .catch((err) => {
      ToastStore.error(err.response.data);
    })
    .finally(() => {
      sendingMark.value = false;
    });
};

const dismiss = () => {
  if (sendingDismiss.value || sendingMark.value || loading.value || !suggestion.value.organizationId) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.DISMISS_FAKE_ORGANIZATION_SUGGESTION,
    type: EventType.FEATURE,
  });

  sendingDismiss.value = true;

  OrganizationService.dismissFakeOrganizationSuggestion(suggestion.value.organizationId)
    .then(() => {
      changed.value = true;
      fetch(currentOffset.value);
    })
    .catch((err) => {
      ToastStore.error(err.response.data);
    })
    .finally(() => {
      sendingDismiss.value = false;
    });
};

onMounted(() => {
  fetch(props.offset);
});

onUnmounted(() => {
  if (changed.value) {
    emit('reload');
  }
});
</script>

<script lang="ts">
export default {
  name: 'AppFakeOrganizationSuggestions',
};
</script>
