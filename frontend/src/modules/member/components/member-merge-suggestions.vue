<template>
  <div class="panel !p-0">
    <!-- Header -->
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
        <div
          v-else-if="hasSuggestion"
          class="text-xs leading-5 text-gray-500"
        >
          <div>Suggestion {{ offset + 1 }}</div>
        </div>
        <div
          v-else
          class="text-xs leading-5 text-gray-500"
        >
          <div>0 suggestions</div>
        </div>
      </div>
      <div class="flex items-center gap-4">
        <app-member-merge-similarity v-if="!loading && membersToMerge.similarity" :similarity="membersToMerge.similarity" />
        <lf-button
          type="secondary"
          :disabled="loading || isEditLockedForSampleData || !hasSuggestion"
          :loading="sendingIgnore"
          @click="ignoreSuggestion()"
        >
          Ignore suggestion
        </lf-button>
        <lf-button
          type="primary"
          :disabled="loading || isEditLockedForSampleData || !hasSuggestion"
          :loading="sendingMerge"
          @click="mergeSuggestion()"
        >
          Merge profile
        </lf-button>
        <slot name="actions" />
      </div>
    </header>

    <div v-if="loading || hasSuggestion">
      <!-- Comparison -->
      <!-- Loading -->
      <div v-if="loading" class="flex p-5">
        <div class="w-1/3 rounded-lg">
          <app-member-merge-suggestions-details
            :member="null"
            :loading="true"
            :is-primary="true"
          />
        </div>
        <div class="w-1/3 -ml-px rounded-lg">
          <app-member-merge-suggestions-details
            :member="null"
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
          v-for="(member, mi) of membersToMerge.members"
          :key="member.id"
          class="w-1/3"
        >
          <app-member-merge-suggestions-details
            :member="member"
            :compare-member="
              membersToMerge.members[(mi + 1) % membersToMerge.members.length]
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
          <app-member-merge-suggestions-details
            :member="preview"
            :is-preview="true"
            class="rounded-lg bg-primary-25"
          />
        </div>
      </div>
    </div>
    <!-- Empty state -->
    <div v-else class="py-20 flex flex-col items-center">
      <lf-icon name="shuffle" :size="160" class="text-gray-200 flex items-center mb-8" />
      <h5 class="text-center text-lg font-semibold mb-4">
        No merge suggestions
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn't find any duplicated profiles
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
import AppMemberMergeSuggestionsDetails from '@/modules/member/components/suggestions/member-merge-suggestions-details.vue';
import { merge } from 'lodash';
import useMemberMergeMessage from '@/shared/modules/merge/config/useMemberMergeMessage';
import LfButton from '@/ui-kit/button/Button.vue';
import AppMemberMergeSimilarity from '@/modules/member/components/suggestions/member-merge-similarity.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { MemberService } from '../member-service';

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

const { trackEvent } = useProductTracking();
const { getContributorMergeActions } = useContributorStore();

const membersToMerge = ref([]);
const primary = ref(0);
const offset = ref(props.offset);
const hasMore = ref(false);
const hasSuggestion = ref(false);
const loading = ref(false);

const sendingIgnore = ref(false);
const sendingMerge = ref(false);

const changed = ref(false);

const bioHeight = ref(0);

const clearMember = (member) => {
  const cleanedMember = { ...member };
  // eslint-disable-next-line no-restricted-syntax
  for (const key in cleanedMember.attributes) {
    if (!cleanedMember.attributes[key].default) {
      delete cleanedMember.attributes[key];
    }
  }
  return cleanedMember;
};

const preview = computed(() => {
  const primaryMember = membersToMerge.value.members[primary.value];
  const secondaryMember = membersToMerge.value.members[(primary.value + 1) % 2];
  const mergedMembers = merge({}, clearMember(secondaryMember), clearMember(primaryMember));
  mergedMembers.identities = [
    ...primaryMember.identities,
    ...secondaryMember.identities,
  ];
  mergedMembers.score = Math.max(primaryMember.score, secondaryMember.score);
  mergedMembers.activityCount = +primaryMember.activityCount + +secondaryMember.activityCount;
  return mergedMembers;
});

const updateSuggestionsState = (res) => {
  hasMore.value = Boolean(res.hasMore);
  const rows = res.rows.filter((suggestion) => suggestion.similarity > 0);

  if (rows.length > 0) {
    hasSuggestion.value = true;
    [membersToMerge.value] = rows;
  } else {
    hasSuggestion.value = false;
    membersToMerge.value = [];
  }
};

const fetch = (page) => {
  if (page > -1) {
    offset.value = page;
  }

  trackEvent({
    key: FeatureEventKey.NAVIGATE_MEMBERS_MERGE_SUGGESTIONS,
    type: EventType.FEATURE,
  });

  loading.value = true;

  return MemberService.fetchMergeSuggestions(1, offset.value, props.query ?? {})
    .then((res) => {
      offset.value = +res.offset;

      updateSuggestionsState(res);

      if (!hasSuggestion.value && offset.value > 0) {
        return fetch(offset.value - 1);
      }

      primary.value = 0;
      return undefined;
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
    key: FeatureEventKey.IGNORE_MEMBER_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: membersToMerge.value.similarity,
    },
  });

  sendingIgnore.value = true;
  MemberService.addToNoMerge(...membersToMerge.value.members)
    .then(() => {
      ToastStore.success('Merging suggestion ignored successfully');
      getContributorMergeActions();
      fetch(offset.value);
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

  sendingMerge.value = true;

  const primaryMember = membersToMerge.value.members[primary.value];
  const secondaryMember = membersToMerge.value.members[(primary.value + 1) % 2];

  const { loadingMessage, apiErrorMessage } = useMemberMergeMessage;

  loadingMessage();

  trackEvent({
    key: FeatureEventKey.MERGE_MEMBER_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: membersToMerge.value.similarity,
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
      primary.value = 0;

      fetch(offset.value);
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
  name: 'AppMemberMergeSuggestionsPage',
};
</script>
