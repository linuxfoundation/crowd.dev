<template>
  <lf-dropdown placement="bottom-end" width="15rem">
    <template #trigger>
      <lf-button
        size="small"
        type="secondary-ghost"
        :icon-only="true"
      >
        <lf-icon name="ellipsis" />
      </lf-button>
    </template>

    <router-link
      :to="{
        name: 'organizationView',
        params: { id: props.suggestion.organizationId },
        query: { projectGroup: selectedProjectGroup?.id },
      }"
      target="_blank"
    >
      <lf-dropdown-item>
        <lf-icon name="eye" />View profile
      </lf-dropdown-item>
    </router-link>

    <lf-dropdown-item @click="dismiss(props.suggestion)">
      <lf-icon name="circle-xmark" />Dismiss suggestion
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

const props = defineProps<{
  suggestion: any,
}>();

const emit = defineEmits<{(e: 'dismiss', suggestion: any): void;}>();

const { trackEvent } = useProductTracking();
const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const dismiss = (suggestion: any) => {
  trackEvent({
    key: FeatureEventKey.DISMISS_FAKE_ORGANIZATION_SUGGESTION,
    type: EventType.FEATURE,
  });

  emit('dismiss', suggestion);
};
</script>

<script lang="ts">
export default {
  name: 'LfFakeOrganizationSuggestionDropdown',
};
</script>
