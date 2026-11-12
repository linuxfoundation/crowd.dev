<template>
  <div class="flex">
    <svg
      v-bind="$attrs"
      @mouseenter="isHovered = true"
      @mouseleave="isHovered = false"
    >
      <use
        :href="`/icons/crowd-icons.svg?v=${version}#${name}`"
        :fill="isHovered && hoverColor ? hoverColor : color"
      />
    </svg>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { dateHelper } from '@/shared/date-helper/date-helper';

defineOptions({ name: 'LfSvg' });

withDefaults(defineProps<{
  name: string;
  color?: string;
  hoverColor?: string | null;
}>(), {
  color: '#9CA3AF',
  hoverColor: null,
});

const isHovered = ref(false);
const version = dateHelper().format('x');
</script>
