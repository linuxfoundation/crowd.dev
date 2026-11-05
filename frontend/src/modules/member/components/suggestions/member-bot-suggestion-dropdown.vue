<template>
  <lf-dropdown placement="bottom-end" width="15rem">
    <template #trigger>
      <lf-button
        size="small"
        type="secondary-ghost"
        icon-only
      >
        <lf-icon name="ellipsis" />
      </lf-button>
    </template>

    <router-link
      :to="{
        name: 'memberView',
        params: { id: props.suggestion.memberId },
        query: { projectGroup: selectedProjectGroup?.id },
      }"
      target="_blank"
    >
      <lf-dropdown-item>
        <lf-icon name="eye" />View profile
      </lf-dropdown-item>
    </router-link>

    <lf-dropdown-item @click="ignore(props.suggestion)">
      <lf-icon name="circle-xmark" />Ignore suggestion
    </lf-dropdown-item>
  </lf-dropdown>
</template>

<script setup lang="ts">
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import type { MemberBotSuggestionRef } from '@/modules/member/types/MemberSuggestion';

const props = defineProps<{
  suggestion: MemberBotSuggestionRef,
}>();

const emit = defineEmits<{(e: 'reload'): void; (e: 'ignoreSuggestion', suggestion: MemberBotSuggestionRef): void;}>();

const { trackEvent } = useProductTracking();
const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const ignore = (suggestion: MemberBotSuggestionRef) => {
  trackEvent({
    key: FeatureEventKey.IGNORE_MEMBER_BOT_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: suggestion.confidence,
    },
  });

  emit('ignoreSuggestion', suggestion);
};

</script>

<script lang="ts">
export default {
  name: 'LfMemberBotSuggestionDropdown',
};
</script>
