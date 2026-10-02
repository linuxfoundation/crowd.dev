<template>
  <div v-if="form">
    <div class="p-4">
      <p class="text-xs text-gray-500 pb-3">
        {{ props.config.label }}
      </p>
      <el-radio-group v-model="form.value" class="flex-col !items-start">
        <!-- eslint-disable-next-line vue/prefer-true-attribute-shorthand -- label accepts non-boolean values -->
        <el-radio :label="true" size="large" class="!mr-0 !h-6 !font-normal !mb-3 flex items-center" data-qa="filter-boolean-true">
          True
        </el-radio>
        <el-radio :label="false" size="large" class="!mr-0 !h-5 !font-normal" data-qa="filter-boolean-false">
          False
        </el-radio>
      </el-radio-group>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue';
import type {
  BooleanFilterValue,
  BooleanFilterOptions,
  BooleanFilterConfig,
} from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';

const props = defineProps<{
  modelValue: BooleanFilterValue,
  config: BooleanFilterConfig,
} & BooleanFilterOptions>();

const emit = defineEmits<{(e: 'update:modelValue', value: BooleanFilterValue): void}>();

const form = computed({
  get: () => props.modelValue,
  set: (value: BooleanFilterValue) => emit('update:modelValue', value),
});

const defaultForm: BooleanFilterValue = {
  value: true,
};

const rules: any = {
  value: {
    required,
  },
};

useVuelidate(rules, form);

onMounted(() => {
  form.value = {
    ...defaultForm,
    ...form.value,
  };
});
</script>

<script lang="ts">
export default {
  name: 'LfBooleanFilter',
};
</script>
