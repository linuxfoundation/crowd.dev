<template>
  <div class="lfx-c-tab" :class="{ 'is-active': isActive }" @click="selectTab">
    <slot />
  </div>
</template>

<script lang="ts" setup>
import { computed, getCurrentInstance } from 'vue';

const props = defineProps<{
  name: string,
}>();

const parent = computed(() => {
  const instance = getCurrentInstance();
  return instance?.parent;
});

const isActive = computed<boolean>(() => parent.value?.props?.modelValue === props.name);

const selectTab = () => {
  parent.value?.emit('update:modelValue', props.name);
};
</script>

<script lang="ts">
export default {
  name: 'LfxTab',
};
</script>
