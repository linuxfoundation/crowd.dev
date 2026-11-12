<template>
  <a
    v-if="activity && activity.url && platform?.activity?.showLink"
    :href="activity.url"
    class="text-2xs text-gray-500 font-medium flex items-center"
    target="_blank"
    rel="noopener noreferrer"
    @click.stop
  >
    <lf-icon name="arrow-up-right-from-square" :size="14" class="mr-1" />
    <span
      v-if="platform"
      class="block"
    >Open on {{ platform.name }}</span>
    <span v-else class="block">Open link</span></a>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { lfIdentities } from '@/config/identities';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Activity } from '@/shared/modules/activity/types/Activity';

defineOptions({ name: 'AppActivityLink' });

const props = withDefaults(defineProps<{
  activity?: Activity;
}>(), {
  activity: () => ({} as Activity),
});

const platform = computed(() => lfIdentities[props.activity.platform]);
</script>
