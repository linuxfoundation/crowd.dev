<template>
  <lf-input v-bind="$attrs" v-model="value" :type="reveal ? 'text' : 'password'">
    <template #suffix>
      <div @click="reveal = !reveal">
        <lf-icon :name="reveal ? 'eye-slash' : 'eye'" :size="16" class="text-gray-400 cursor-pointer" />
      </div>
    </template>
  </lf-input>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const value = computed<string>({
  get() {
    return props.modelValue;
  },
  set(val: string) {
    emit('update:modelValue', val);
  },
});

const reveal = ref<boolean>(false);
</script>

<script lang="ts">
export default {
  name: 'LfInputPassword',
};
</script>
