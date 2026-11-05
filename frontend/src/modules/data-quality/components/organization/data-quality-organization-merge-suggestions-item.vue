<template>
  <article class="border-b border-gray-100 py-5 flex items-center justify-between">
    <div class="flex items-center gap-4">
      <template v-for="(organization, mi) of props.suggestion.organizations" :key="organization.id">
        <div v-if="mi > 0">
          <lf-icon name="arrow-left" :size="16" class="text-gray-400" />
        </div>
        <div class="flex items-center gap-3">
          <lf-avatar
            :src="organization.logo ?? undefined"
            :name="organization.displayName"
            :size="32"
            img-class="!object-contain"
          >
            <template #placeholder>
              <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                <lf-icon name="house-building" :size="16" class="text-gray-400" />
              </div>
            </template>
          </lf-avatar>
          <p class="text-medium font-semibold truncate" style="max-width: 30ch">
            {{ organization.displayName }}
          </p>
        </div>
      </template>
      <div class="flex gap-2">
        <lf-badge type="primary" size="small" class="!font-semibold">
          Merge suggestion
        </lf-badge>
        <app-member-merge-similarity :similarity="+props.suggestion.similarity" percentage-only />
      </div>
    </div>
    <slot name="action" />
  </article>
</template>

<script lang="ts" setup>
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import AppMemberMergeSimilarity from '@/modules/member/components/suggestions/member-merge-similarity.vue';
import type { DataQualityOrganizationMergeSuggestion } from '@/modules/data-quality/types/DataQualitySuggestion';

const props = defineProps<{
  suggestion: DataQualityOrganizationMergeSuggestion,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityOrganizationMergeSuggestionsItem',
};
</script>
