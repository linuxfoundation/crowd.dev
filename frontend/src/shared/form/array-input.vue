<template>
  <article class="pb-3 flex items-center">
    <div class="flex flex-grow items-start">
      <app-form-item
        :validation="$v"
        :error-messages="{
          required: 'This field is required',
        }"
        class="mb-0 mr-2 no-margin flex-grow is-error-relative"
      >
        <div :class="{ 'input-label-container': !!inputLabel }">
          <span v-if="inputLabel">{{ inputLabel }}</span>
          <el-input
            v-model="model"
            :placeholder="placeholder"
            :disabled="disabled"
            :class="[{ 'opacity-50': disabled }, inputClass]"
            @blur="$v.$touch"
            @change="$v.$touch"
          />
        </div>
      </app-form-item>
    </div>
    <slot name="after" />
  </article>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { required } from '@vuelidate/validators';
import useVuelidate, { type ValidationArgs } from '@vuelidate/core';
import AppFormItem from '@/shared/form/form-item.vue';

defineOptions({ name: 'AppArrayInput' });

const props = withDefaults(defineProps<{
  modelValue: string;
  placeholder?: string | null;
  disabled?: boolean;
  inputClass?: string;
  inputLabel?: string;
}>(), {
  placeholder: null,
  disabled: false,
  inputClass: '',
  inputLabel: '',
});

const emit = defineEmits<{(e: 'update:modelValue', value: string): void;
}>();

defineSlots<{
  after?:() => unknown;
}>();

const rules = {
  required,
};

const model = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    emit('update:modelValue', value);
  },
});

const $v = useVuelidate<string, ValidationArgs>(rules, model);
</script>
