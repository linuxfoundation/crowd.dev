<template>
  <article class="border-b border-gray-100 py-5 flex items-center justify-between">
    <div class="flex items-center gap-4">
      <div class="flex items-center gap-3">
        <lf-avatar
          :src="suggestion.logo"
          :name="suggestion.displayName"
          :size="32"
          img-class="!object-contain"
        >
          <template #placeholder>
            <div class="w-full h-full bg-gray-50 flex items-center justify-center">
              <lf-icon name="house-building" :size="16" class="text-gray-400" />
            </div>
          </template>
        </lf-avatar>
        <div>
          <p class="text-medium font-semibold truncate" style="max-width: 30ch">
            {{ suggestion.displayName }}
          </p>
        </div>
      </div>
      <div class="flex gap-2 items-center">
        <lf-badge type="warning" size="small" class="!font-semibold">
          Fake organization
        </lf-badge>
        <span class="text-small text-gray-500">
          {{ formatNumberToCompact(+suggestion.activityCount) }} activities
        </span>
      </div>
    </div>
    <slot name="action" />
  </article>
</template>

<script lang="ts" setup>
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { formatNumberToCompact } from '@/utils/number';

defineProps<{
  suggestion: any,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityFakeOrganizationSuggestionItem',
};
</script>
