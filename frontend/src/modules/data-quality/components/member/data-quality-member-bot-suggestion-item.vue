<template>
  <article class="border-b border-gray-100 py-5 flex items-center justify-between">
    <div class="flex items-center gap-4">
      <div class="flex items-center gap-3">
        <lf-avatar
          :src="suggestion.avatarUrl ?? undefined"
          :name="suggestion.displayName"
          :size="32"
        />
        <div>
          <p class="text-medium font-semibold truncate" style="max-width: 30ch">
            {{ suggestion.displayName }}
          </p>
        </div>
      </div>
      <div class="flex gap-2">
        <lf-badge type="warning" size="small" class="!font-semibold">
          Bot suggestion
        </lf-badge>
        <app-member-merge-similarity :similarity="+props.suggestion.confidence" percentage-only />
      </div>
    </div>
    <slot name="action" />
  </article>
</template>

<script lang="ts" setup>
import type { DataQualityMemberBotSuggestion } from '@/modules/data-quality/types/DataQualitySuggestion';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import AppMemberMergeSimilarity from '@/modules/member/components/suggestions/member-merge-similarity.vue';

const props = defineProps<{
  suggestion: DataQualityMemberBotSuggestion,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityMemberBotSuggestionItem',
};
</script>
