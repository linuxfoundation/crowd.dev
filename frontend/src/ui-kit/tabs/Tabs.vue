<template>
  <div class="c-tabs" :class="`c-tabs--${props.size}`">
    <slot />
  </div>
</template>

<script setup lang="ts">
import {
  computed, onMounted, watch,
} from 'vue';
import { useRoute, useRouter } from 'vue-router';
import type { TabsSize } from '@/ui-kit/tabs/types/TabsSize';

const props = withDefaults(defineProps<{
  modelValue?: string,
  size?: TabsSize,
  fragment?: boolean,
}>(), {
  modelValue: '',
  size: 'medium',
  fragment: true,
});

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const route = useRoute();
const router = useRouter();

const model = computed({
  get() {
    return props.modelValue || '';
  },
  set(value: string) {
    emit('update:modelValue', value);
  },
});

watch(() => props.modelValue, () => {
  if (props.fragment) {
    router.replace({
      ...route,
      hash: `#${props.modelValue}`,
    });
  }
});

const readHash = () => {
  if (!props.fragment) {
    return;
  }
  const hash = route?.hash.replace('#', '');
  if (hash && hash !== model.value) {
    emit('update:modelValue', hash);
  }
};

onMounted(() => {
  readHash();
});

watch(() => route?.hash, () => {
  readHash();
});
</script>

<script lang="ts">
export default {
  name: 'LfTabs',
};
</script>
