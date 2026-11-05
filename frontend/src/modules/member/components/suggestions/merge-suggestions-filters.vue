<template>
  <div class="flex items-center gap-3">
    <app-merge-suggestions-search
      v-model="form.search"
      :placeholder="props.placeholder"
    />
    <app-merge-suggestions-confidence-filter
      v-model="form.confidence"
    />
    <app-merge-suggestions-projects-filter
      v-model:segments="form.segments"
      v-model:child-segments="form.childSegments"
    />
  </div>
</template>

<script setup lang="ts">
import { reactive, watch } from 'vue';
import AppMergeSuggestionsConfidenceFilter
  from '@/modules/member/components/suggestions/filters/merge-suggestions-confidence-filter.vue';
import AppMergeSuggestionsSearch from '@/modules/member/components/suggestions/filters/merge-suggestions-search.vue';
import type { MemberMergeSuggestionsQuery } from '@/modules/member/types/MemberSuggestion';
import AppMergeSuggestionsProjectsFilter
  from '@/modules/member/components/suggestions/filters/merge-suggestions-projects-filter.vue';

const props = defineProps<{
  placeholder?: string;
}>();

const emit = defineEmits<{(e: 'search', value: MemberMergeSuggestionsQuery): void}>();

const form = reactive<{
  search: string;
  segments: string[],
  childSegments: string[],
  confidence: string[],
}>({
  search: '',
  segments: [],
  childSegments: [],
  confidence: ['high'],
});

watch(() => form, (form) => {
  const subprojects = form.childSegments.length ? form.childSegments : undefined;
  const projects = form.segments.length ? form.segments : subprojects;

  emit('search', {
    similarity: form.confidence,
    displayName: form.search || undefined,
    projectIds: projects,
    subprojectIds: subprojects,
  });
}, { deep: true, immediate: true });

</script>

<script lang="ts">
export default {
  name: 'AppMergeSuggestionsFilters',
};
</script>
