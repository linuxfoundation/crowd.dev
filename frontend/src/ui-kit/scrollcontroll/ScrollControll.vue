<template>
  <div class="relative overflow-auto" v-bind="$attrs" @scroll="controlScroll">
    <slot />
  </div>
</template>

<script setup lang="ts">
const emit = defineEmits<{(e: 'bottom'): void}>();

const controlScroll = (e: Event) => {
  const target = e.target as HTMLElement;
  if (target.scrollTop + target.clientHeight >= target.scrollHeight - 10) {
    emit('bottom');
  }
};
</script>

<script lang="ts">
export default {
  name: 'LfScrollControll',
};
</script>
