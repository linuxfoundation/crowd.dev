<template>
  <article class="mb-5">
    <lf-field label-text="Documentation URL override">
      <p class="text-tiny text-gray-500 mb-2">
        Set the docs URL by hand, mark the project as having no docs, or clear
        the override to let discovery run again.
      </p>
      <p class="text-small mb-2" data-testid="docs-override-status">
        {{ statusText }}
      </p>
      <lf-input
        v-model="docsUrl"
        class="h-10"
        placeholder="https://docs.example.com"
        :invalid="!!docsUrl && !isValidDocsUrl"
        :disabled="isPending"
      />
      <p v-if="docsUrl && !isValidDocsUrl" class="text-tiny text-red-500 mt-1">
        Enter a valid http(s) URL
      </p>
      <div class="flex items-center gap-2 mt-3">
        <lf-button
          type="primary"
          size="small"
          :disabled="!isValidDocsUrl || isPending"
          @click="setMutation.mutate({ docsUrl: docsUrl.trim() })"
        >
          Save URL
        </lf-button>
        <lf-button
          type="secondary"
          size="small"
          :disabled="isPending"
          @click="setMutation.mutate({ noDocs: true })"
        >
          Mark as no docs
        </lf-button>
        <lf-button
          type="secondary-ghost"
          size="small"
          :disabled="!docsOverride || isPending"
          @click="clearMutation.mutate()"
        >
          Clear override
        </lf-button>
      </div>
    </lf-field>
  </article>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import { useMutation, useQueryClient } from '@tanstack/vue-query';
import LfButton from '@/ui-kit/button/Button.vue';
import LfField from '@/ui-kit/field/Field.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import { ToastStore } from '@/shared/message/notification';
import { TanstackKey } from '@/shared/types/tanstack';
import type {
  InsightsProjectDocsOverride,
  InsightsProjectDocsOverrideRequest,
} from '../../models/insights-project.model';
import { INSIGHTS_PROJECTS_SERVICE } from '../../services/insights-projects.service';
import { isHttpUrl } from '../../insight-project-helper';

defineOptions({ name: 'LfInsightsProjectDocsOverride' });

const props = defineProps<{
  insightsProjectId: string;
  docsOverride?: InsightsProjectDocsOverride | null;
}>();

const emit = defineEmits<{(e: 'change', value: InsightsProjectDocsOverride | null): void }>();

const queryClient = useQueryClient();
const docsUrl = ref('');

const isValidDocsUrl = computed(() => isHttpUrl(docsUrl.value));

const statusText = computed(() => {
  if (!props.docsOverride) {
    return 'No override set';
  }
  return props.docsOverride.docsUrl
    ? `Active override: ${props.docsOverride.docsUrl}`
    : 'Active override: project has no docs';
});

const markStale = () => queryClient.invalidateQueries({
  queryKey: [TanstackKey.ADMIN_INSIGHTS_PROJECTS],
  refetchType: 'none',
});

const setMutation = useMutation({
  mutationFn: (request: InsightsProjectDocsOverrideRequest) => INSIGHTS_PROJECTS_SERVICE.setDocsOverride(props.insightsProjectId, request),
  onSuccess: (saved) => {
    emit('change', saved);
    docsUrl.value = '';
    ToastStore.closeAll();
    ToastStore.success('Docs override saved');
    markStale();
  },
  onError: () => {
    ToastStore.closeAll();
    ToastStore.error('Something went wrong while saving the docs override');
  },
});

const clearMutation = useMutation({
  mutationFn: () => INSIGHTS_PROJECTS_SERVICE.clearDocsOverride(props.insightsProjectId),
  onSuccess: () => {
    emit('change', null);
    ToastStore.closeAll();
    ToastStore.success('Docs override cleared');
    markStale();
  },
  onError: () => {
    ToastStore.closeAll();
    ToastStore.error('Something went wrong while clearing the docs override');
  },
});

const isPending = computed(() => setMutation.isPending.value || clearMutation.isPending.value);
</script>
