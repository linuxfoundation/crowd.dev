<template>
  <article>
    <app-form-item class="mb-0">
      <div class="flex flex-grow gap-2 last:pb-0">
        <el-input
          v-model="model"
          type="tel"
          placeholder="Enter phone number"
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
import AppFormItem from '@/shared/form/form-item.vue';

defineOptions({
  name: 'AppOrganizationFormPhoneNumberItem',
});

const props = defineProps<{
  modelValue: string;
}>();

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
</script>
