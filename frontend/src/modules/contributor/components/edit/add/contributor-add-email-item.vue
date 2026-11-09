<template>
  <div class="w-full">
    <div class="flex justify-between w-full">
      <lf-input
        v-model="form"
        class="w-full h-10"
        v-bind="$attrs"
        :invalid="$v.form.$invalid && $v.form.$dirty"
        @change="$v.form.$touch()"
        @blur="$v.form.$touch()"
      />
      <slot />
    </div>
    <lf-field-messages :validation="$v.form" :error-messages="{ email: 'Invalid email' }" />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfInput from '@/ui-kit/input/Input.vue';
import useVuelidate from '@vuelidate/core';
import { email } from '@vuelidate/validators';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';

defineOptions({
  name: 'LfContributorAddEmailItem',
});

const props = defineProps<{
  modelValue: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const form = computed<string>({
  get: () => props.modelValue,
  set: (value) => emit('update:modelValue', value),
});

const rules = {
  form: {
    email,
  },
};

const $v = useVuelidate(rules, { form });
</script>
