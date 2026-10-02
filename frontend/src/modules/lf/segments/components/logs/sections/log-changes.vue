<template>
  <section v-if="hasChanges" class="py-6 border-t border-gray-200">
    <h4 class="text-base font-semibold pb-4">
      Changes
    </h4>
    <article
      v-for="(remove, ri) of (changes?.removals || [])"
      :key="ri"
      class="pb-2 flex items-center text-sm"
    >
      <lf-icon name="minus" :size="16" class="text-red-500 mr-2" />
      <p class="c-changes" v-html="$sanitize(remove)" />
    </article>
    <article
      v-for="(add, ai) of (changes?.additions || [])"
      :key="ai"
      class="pb-2 flex items-center text-sm"
    >
      <lf-icon name="plus" :size="16" class="text-green-500 mr-2" />
      <p class="c-changes" v-html="$sanitize(add)" />
    </article>
    <article
      v-for="(change, ci) of (changes?.changes || [])"
      :key="ci"
      class="pb-2 flex items-center text-sm"
    >
      <lf-icon name="arrows-rotate-reverse" :size="16" class="text-primary-500 mr-2" />
      <p class="c-changes" v-html="$sanitize(change)" />
    </article>
  </section>
</template>

<script setup lang="ts">
import type { AuditLog } from '@/modules/lf/segments/types/AuditLog';
import { computed, onMounted, ref } from 'vue';
import { logRenderingConfig } from '@/modules/lf/config/audit-logs/log-rendering';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  log: AuditLog
}>();

const changes = ref<Record<string, any[]> | null>({
  removals: [],
  additions: [],
  changes: [],
});

const getChanges = async () => {
  if (logRenderingConfig[props.log.actionType]?.changes) {
    changes.value = await logRenderingConfig[props.log.actionType]?.changes?.(props.log) || null;
  } else {
    changes.value = {
      removals: [],
      additions: [],
      changes: [],
    };
  }
};

const hasChanges = computed(() => {
  const c = changes.value;
  return [...c.removals, ...c.additions, ...c.changes].length > 0;
});

onMounted(() => {
  getChanges();
});
</script>

<script lang="ts">
export default {
  name: 'AppLfAuditLogsChanges',
};
</script>

<style lang="scss">
.c-changes {
  span{
    @apply text-gray-700;
  }
}
</style>
