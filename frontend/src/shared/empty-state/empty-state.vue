<template>
  <div class="flex gap-6 items-center justify-center">
    <lf-icon :name="icon" class="text-gray-300" :size="40" />
    <div class="text-gray-400 italic text-xs">
      {{ description }}
    </div>
  </div>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineProps<{
  icon: string;
  description: string;
}>();
</script>
