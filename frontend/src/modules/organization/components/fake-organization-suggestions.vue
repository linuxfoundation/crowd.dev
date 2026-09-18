<template>
  <div class="panel !p-0">
    <header class="flex items-center justify-between px-6 py-5 border-b">
      <div class="flex items-center gap-4">
        <div class="flex items-center gap-2">
          <lf-button
            type="secondary"
            size="small"
            :disabled="loading || offset <= 0 || !hasSuggestion"
            :icon-only="true"
            @click="fetch(offset - 1)"
          >
            <lf-icon name="chevron-left" :size="16" />
          </lf-button>
          <lf-button
            type="secondary"
            size="small"
            :disabled="loading || !hasMore"
            :icon-only="true"
            @click="fetch(offset + 1)"
          >
            <lf-icon name="chevron-right" :size="16" />
          </lf-button>
        </div>

        <app-loading v-if="loading" height="16px" width="128px" radius="3px" />
        <div v-else class="text-xs leading-5 text-gray-500">
          <div>{{ hasSuggestion ? `Suggestion ${offset + 1}` : '0 suggestions' }}</div>
        </div>
      </div>
      <div class="flex items-center gap-4">
        <lf-button
          type="secondary"
          size="small"
          :disabled="loading || !hasSuggestion"
          :loading="sendingDismiss"
          @click="dismiss()"
        >
          Dismiss suggestion
        </lf-button>
        <lf-button
          type="primary"
          size="small"
          :disabled="loading || !hasSuggestion"
          :loading="sendingMark"
          @click="markAsFake()"
        >
          Mark as fake
        </lf-button>
        <slot name="actions" />
      </div>
    </header>

    <div v-if="loading || hasSuggestion" class="p-5">
      <app-organization-merge-suggestions-details
        :organization="suggestion.organization"
        :loading="loading"
      >
        <template #header>
          <div class="h-13" />
        </template>
      </app-organization-merge-suggestions-details>
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
  }>(),
  {
    offset: 0,
  },
);

const emit = defineEmits<{(e: 'reload'): void}>();

const { trackEvent } = useProductTracking();

const suggestion = ref<any>({});
const offset = ref(props.offset);
const hasMore = ref(false);
const hasSuggestion = ref(false);
const loading = ref(false);
const sendingDismiss = ref(false);
const sendingMark = ref(false);
const changed = ref(false);

const fetch = (page: number) => {
  if (page > -1) {
    offset.value = page;
  }

  loading.value = true;

  return OrganizationService.fetchFakeOrganizationSuggestions(1, offset.value)
    .then((res: any) => {
      offset.value = +res.offset;
      hasMore.value = Boolean(res.hasMore);
      const [row] = res.rows || [];

      if (row?.organization) {
        hasSuggestion.value = true;
        suggestion.value = row;
        return undefined;
      }

      hasSuggestion.value = false;
      suggestion.value = {};

      if (offset.value > 0) {
        return fetch(offset.value - 1);
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
      fetch(offset.value);
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
      fetch(offset.value);
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
