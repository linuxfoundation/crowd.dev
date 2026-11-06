<template>
  <el-tooltip
    v-if="sentiment && sentiment > 0"
    effect="dark"
    :content="`Sentiment score: ${sentiment}`"
    placement="top"
  >
    <lf-icon v-if="label === 'Positive'" name="face-smile" :size="14" class="text-green-600" />
    <lf-icon v-else-if="label === 'Neutral'" name="face-meh" :size="14" class="text-gray-400" />
    <lf-icon v-else name="face-frown" :size="14" class="text-red-500" />
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'AppActivitySentiment',
});

const props = withDefaults(defineProps<{
  sentiment?: number;
}>(), {
  sentiment: 0,
});

const label = computed(() => {
  if (props.sentiment >= 67) {
    return 'Positive';
  } if (props.sentiment <= 33) {
    return 'Negative';
  }
  return 'Neutral';
});

</script>
