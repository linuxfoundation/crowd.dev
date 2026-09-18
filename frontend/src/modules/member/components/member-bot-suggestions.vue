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
        <app-member-merge-similarity
          v-if="!loading && suggestion.confidence"
          :similarity="suggestion.confidence"
        />
        <lf-button
          type="secondary"
          :disabled="loading || !hasSuggestion"
          :loading="sendingIgnore"
          @click="markAsBot(false)"
        >
          Ignore suggestion
        </lf-button>
        <lf-button
          type="primary"
          :disabled="loading || !hasSuggestion"
          :loading="sendingMark"
          @click="markAsBot(true)"
        >
          Mark as bot
        </lf-button>
        <slot name="actions" />
      </div>
    </header>

    <div v-if="loading || hasSuggestion">
      <div class="flex p-5">
        <div class="w-full">
          <app-member-merge-suggestions-details
            :member="suggestion.member"
            :loading="loading"
            :is-preview="true"
            class="rounded-lg bg-primary-25"
          />
        </div>
      </div>
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
const sendingIgnore = ref(false);
const sendingMark = ref(false);
const changed = ref(false);

const suggestionQuery = () => (props.segments?.length ? { segments: props.segments } : {});

const fetch = (page: number) => {
  if (page > -1) {
    currentOffset.value = page;
  }

  loading.value = true;

  return MemberService.fetchBotSuggestions(1, currentOffset.value, suggestionQuery())
    .then((res: any) => {
      currentOffset.value = +res.offset;
      hasMore.value = Boolean(res.hasMore);
      const [row] = res.rows || [];

      if (row?.memberId) {
        hasSuggestion.value = true;
        suggestion.value = {
          ...row,
          member: row.member ?? {
            id: row.memberId,
            displayName: row.displayName,
            attributes: row.attributes,
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
      fetch(currentOffset.value);
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
