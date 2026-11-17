<template>
  <article>
    <app-form-item
      :validation="$v"
      :error-messages="{
        email: 'Invalid email',
      }"
      class="mb-0"
      error-class="relative top-0"
    >
      <div class="flex flex-grow gap-2 last:pb-0">
        <el-input
          v-model="model"
          placeholder="Enter email address"
          @change="$v.$touch()"
          @blur="$v.$touch()"
        />
        <slot name="actions" />
      </div>
    </app-form-item>
  </article>
</template>

<script setup lang="ts">
import {
  computed,
} from 'vue';
import type { ValidationArgs } from '@vuelidate/core';
import useVuelidate from '@vuelidate/core';
import { email } from '@vuelidate/validators';
import AppFormItem from '@/shared/form/form-item.vue';

defineOptions({
  name: 'AppOrganizationFormEmailsItem',
});

const props = withDefaults(defineProps<{
  modelValue?: string;
}>(), {
  modelValue: '',
});

const emit = defineEmits<{(e: 'update:modelValue', value: string): void }>();

defineSlots<{
  actions?:() => unknown;
}>();

const model = computed({
  get() {
    return props.modelValue;
  },
  set(contact: string) {
    emit('update:modelValue', contact);
  },
});

const rules: ValidationArgs = {
  email,
};

const $v = useVuelidate(rules, model);
</script>
