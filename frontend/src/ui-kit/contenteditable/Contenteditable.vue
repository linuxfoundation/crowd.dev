<template>
  <div
    v-bind="$attrs"
    ref="editable"
    contenteditable
    class="outline-none"
    @input="onInput"
  />
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue';

const props = defineProps<{
  modelValue: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const editable = ref<HTMLElement | null>(null);
const proxy = ref<string>(props.modelValue);
const onInput = (e: Event) => {
  // shortcut: cast hides a null target; input events always have one. revisit: if onInput is reused
  const value = (e.target as HTMLElement).innerText;
  proxy.value = value;
  emit('update:modelValue', value);
};

const setDefualtValue = () => {
  if (editable.value) {
    editable.value.innerText = props.modelValue;
  }
};

watch(() => props.modelValue, () => {
  if (proxy.value !== props.modelValue) {
    setDefualtValue();
  }
});

onMounted(() => {
  setDefualtValue();
});
</script>

<script lang="ts">
export default {
  name: 'LfConteneditable',
};
</script>
