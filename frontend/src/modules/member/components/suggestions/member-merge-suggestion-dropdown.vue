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

    <lf-dropdown-item @click="merge(props.suggestion)">
      <lf-icon name="shuffle" />Merge suggestion
    </lf-dropdown-item>

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
import useMemberMergeMessage from '@/shared/modules/merge/config/useMemberMergeMessage';
import { MemberService } from '@/modules/member/member-service';

import { ToastStore } from '@/shared/message/notification';
import { ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';

const props = defineProps<{
  suggestion: any,
}>();

const emit = defineEmits<{(e: 'reload'): void}>();

const { trackEvent } = useProductTracking();

const sending = ref<string>('');

const merge = (suggestion: any) => {
  if (sending.value.length) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.MERGE_MEMBER_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: suggestion.similarity,
    },
  });

  const primaryMember = suggestion.members[0];
  const secondaryMember = suggestion.members[1];
  sending.value = `${primaryMember.id}:${secondaryMember.id}`;

  const { loadingMessage } = useMemberMergeMessage;

  loadingMessage();

  MemberService.merge(primaryMember, secondaryMember)
    .then(() => {
      ToastStore.closeAll();
      ToastStore.info(
        "We're finalizing profiles merging. We will let you know once the process is completed.",
        {
          title: 'Profiles merging in progress',
        },
      );
    })
    .finally(() => {
      emit('reload');
      sending.value = '';
    });
};

const ignore = (suggestion: any) => {
  if (sending.value.length) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.IGNORE_MEMBER_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: suggestion.similarity,
    },
  });

  const primaryMember = suggestion.members[0];
  const secondaryMember = suggestion.members[1];
  sending.value = `${primaryMember.id}:${secondaryMember.id}`;
  MemberService.addToNoMerge(...suggestion.members)
    .then(() => {
      ToastStore.success('Merging suggestion ignored successfully');
      emit('reload');
    })
    .finally(() => {
      sending.value = '';
    });
};

</script>

<script lang="ts">
export default {
  name: 'LfMemberMergeSuggestionDropdown',
};
</script>
