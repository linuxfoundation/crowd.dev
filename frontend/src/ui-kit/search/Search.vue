<template>
  <lf-input v-model="valueProxy">
    <template #prefix>
      <lf-icon name="search" />
    </template>
    <template v-if="typeof valueProxy === 'string' && valueProxy.length" #suffix>
      <div @click="valueProxy = ''">
        <lf-icon name="xmark-circle" type="regular" class="text-gray-300" />
      </div>
    </template>
  </lf-input>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue';
import { debounce } from 'lodash';
import LfInput from '@/ui-kit/input/Input.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: string | number,
  lazy?: boolean,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string | number): void}>();

const valueProxy = ref<string | number>(props.modelValue);

const emitValue = (value: string | number) => emit('update:modelValue', value);

const debouncedEmitValue = debounce(emitValue, 300);

watch(valueProxy, (newVal) => {
  if (props.lazy) {
    debouncedEmitValue(newVal);
  } else {
    emitValue(newVal);
  }
});

watch(() => props.modelValue, (newVal) => {
  valueProxy.value = newVal;
});
</script>

<script lang="ts">
export default {
  name: 'LfSearch',
};
</script>
