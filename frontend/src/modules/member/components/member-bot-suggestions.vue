<template>
  <div class="panel !p-0">
    <header class="sticky top-0 z-10 bg-white border-b">
      <div class="flex items-center justify-between gap-4 px-4 py-4 sm:px-6">
        <div class="flex min-w-0 items-center gap-3">
          <div class="flex shrink-0 items-center gap-2">
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

          <app-loading v-if="loading" height="16px" width="96px" radius="3px" />
          <div v-else class="truncate whitespace-nowrap text-xs leading-5 text-gray-500">
            {{ hasSuggestion ? `Suggestion ${offset + 1}` : '0 suggestions' }}
          </div>
        </div>
        <slot name="actions" />
      </div>

      <div class="flex flex-col gap-3 border-t bg-gray-50 px-4 py-3 sm:flex-row sm:items-center sm:justify-between sm:px-6">
        <div v-if="!loading && suggestion.confidence">
          <app-member-merge-similarity
            :similarity="suggestion.confidence"
          />
        </div>
        <div class="flex w-full items-center gap-2 sm:w-auto">
          <lf-button
            type="secondary"
            size="small"
            class="flex-1 sm:flex-none"
            :disabled="loading || !hasSuggestion"
            :loading="sendingIgnore"
            @click="markAsBot(false)"
          >
            Ignore suggestion
          </lf-button>
          <lf-button
            type="primary"
            size="small"
            class="flex-1 sm:flex-none"
            :disabled="loading || !hasSuggestion"
            :loading="sendingMark"
            @click="markAsBot(true)"
          >
            Mark as bot
          </lf-button>
        </div>
      </div>
    </header>

    <div v-if="loading || hasSuggestion">
      <app-member-merge-suggestions-details
        :member="suggestion.member"
        :loading="loading"
        :is-standalone="true"
      />
    </div>
    <div v-else class="py-20 flex flex-col items-center">
      <lf-icon name="shuffle" :size="160" class="text-gray-200 flex items-center mb-8" />
      <h5 class="text-center text-lg font-semibold mb-4">
        No bot suggestions
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn't find any bot suggestions
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import AppLoading from '@/shared/loading/loading-placeholder.vue';
import AppMemberMergeSuggestionsDetails from '@/modules/member/components/suggestions/member-merge-suggestions-details.vue';
import AppMemberMergeSimilarity from '@/modules/member/components/suggestions/member-merge-similarity.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { ToastStore } from '@/shared/message/notification';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { MemberService } from '@/modules/member/member-service';

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
const sendingIgnore = ref(false);
const sendingMark = ref(false);
const changed = ref(false);

const fetch = (page: number) => {
  if (page > -1) {
    offset.value = page;
  }

  loading.value = true;

  return MemberService.fetchBotSuggestions(1, offset.value)
    .then((res: any) => {
      offset.value = +res.offset;
      hasMore.value = Boolean(res.hasMore);
      const [row] = res.rows || [];

      if (row?.member) {
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
        'There was an error fetching bot suggestion, please try again later',
      );
    })
    .finally(() => {
      loading.value = false;
    });
};

const markAsBot = (bot: boolean) => {
  if (sendingIgnore.value || sendingMark.value || loading.value || !suggestion.value.memberId) {
    return;
  }

  trackEvent({
    key: bot
      ? FeatureEventKey.MARK_MEMBER_BOT_SUGGESTION
      : FeatureEventKey.IGNORE_MEMBER_BOT_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: suggestion.value.confidence,
    },
  });

  if (bot) {
    sendingMark.value = true;
  } else {
    sendingIgnore.value = true;
  }

  MemberService.updateAttributes(suggestion.value.memberId, {
    ...suggestion.value.attributes,
    isBot: {
      custom: bot,
      default: bot,
    },
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
      sendingIgnore.value = false;
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
  name: 'AppMemberBotSuggestions',
};
</script>
