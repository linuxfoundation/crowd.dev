<template>
  <div class="flex items-center gap-1" :class="style.color">
    <lf-icon :name="style.icon" :size="16" type="solid" />
    <p class="text-small leading-4 whitespace-nowrap">
      {{ percentage }}%<span v-if="!props.percentageOnly"> confidence</span>
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'AppMemberMergeSimilarity',
});

const props = defineProps<{
  similarity: number,
  percentageOnly?: boolean
}>();

const percentage = computed(() => Math.ceil(props.similarity * 100));

const style = computed<{
  color: string,
  icon: string
}>(() => {
  if (props.similarity >= 0.9) {
    return {
      color: 'text-green-600',
      icon: 'signal-bars-strong',
    };
  }
  if (props.similarity >= 0.7) {
    return {
      color: 'text-primary-500',
      icon: 'signal-bars-good',
    };
  }
  return {
    color: 'text-yellow-600',
    icon: 'signal-bars-fair',
  };
});
</script>
