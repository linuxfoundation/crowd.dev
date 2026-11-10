<template>
  <el-input
    v-model="model"
    :type="props.type"
    class="filter-input-field"
    :placeholder="placeholder"
  >
    <template v-if="prefix" #prefix>
      <span class="text-gray-600">{{ prefix }}</span>
    </template>

    <template v-if="suffix" #suffix>
      <span class="text-gray-600">{{ suffix }}</span>
    </template>
  </el-input>
</template>

<script setup lang="ts">
import { computed } from 'vue';

defineOptions({
  name: 'LfFilterInput',
});

const props = defineProps<{
  modelValue: string | number | undefined,
  placeholder?: string,
  suffix?: string,
  prefix?: string,
  type?: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string | number): void}>();

const model = computed<string | number>({
  get() {
    // shortcut: callers may pass undefined; cast keeps runtime unchanged. revisit: when filter models get defaults
    return props.modelValue as string | number;
  },
  set(value: string | number) {
    emit('update:modelValue', props.type === 'number' ? +value : value);
  },
});
</script>

<style lang="scss">
.filter-input-field {
  .el-input__wrapper,
  .el-input__wrapper.is-focus,
  .el-input__wrapper:hover {
    @apply bg-gray-50 shadow-none rounded-md border border-gray-50 transition #{!important};

    input {
      &,
      &:hover,
      &:focus {
        border: none !important;
        @apply bg-gray-50 shadow-none outline-none h-full min-h-8;
      }
    }
  }
}

.el-form-item.is-error {
  .filter-input-field {
    .el-input__wrapper,
    .el-input__wrapper.is-focus,
    .el-input__wrapper:hover{
      border-color: var(--el-color-danger) !important;
    }
  }
}

</style>
