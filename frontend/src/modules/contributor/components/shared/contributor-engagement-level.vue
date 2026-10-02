<template>
  <div class="flex items-center gap-1.5 font-semibold text-small">
    <div v-if="!data" class="flex items-center text-secondary-400">
      <div class="animate-spin mr-1.5">
        <lf-icon name="circle-notch" type="solid" :size="16" class="text-secondary-400" />
      </div>
      Calculating
    </div>
    <template v-else>
      <div class="text-white h-4 px-1 min-w-4 flex justify-center rounded" :class="data.bg">
        {{ props.contributor.score }}
      </div>
      <p :class="data.color">
        {{ data.label }}
      </p>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';

const props = defineProps<{
  contributor: Contributor,
}>();

const data = computed<{
  label: string,
  color: string,
  bg: string,
} | null>(() => {
  const { score } = props.contributor;
  if (score < 0) {
    return null;
  }
  if (score <= 1) {
    return {
      label: 'Silent',
      color: 'text-red-500',
      bg: 'bg-red-500',
    };
  }
  if (score <= 3) {
    return {
      label: 'Quiet',
      color: 'text-yellow-500',
      bg: 'bg-yellow-500',
    };
  }
  if (score <= 6) {
    return {
      label: 'Engaged',
      color: 'text-green-500',
      bg: 'bg-green-500',
    };
  }
  if (score <= 8) {
    return {
      label: 'Fan',
      color: 'text-primary-500',
      bg: 'bg-primary-500',
    };
  }
  if (score <= 10) {
    return {
      label: 'Ultra',
      color: 'text-purple-500',
      bg: 'bg-purple-500',
    };
  }
  return null;
});

</script>

<script lang="ts">
export default {
  name: 'LfContributorEngagementLevel',
};
</script>
