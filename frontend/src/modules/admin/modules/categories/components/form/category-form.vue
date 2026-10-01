<template>
  <lf-modal v-model="isModalOpen">
    <div class="px-6 pt-6 pb-8">
      <header class="pb-4">
        <h5>Add category</h5>
      </header>
      <lf-field label-text="Category name" required>
        <lf-input
          v-model="form.name"
          :invalid="$v.name.$invalid && $v.name.$dirty"
          @blur="$v.name.$touch()"
          @change="$v.name.$touch()"
        />
        <lf-field-messages
          :validation="$v.name"
          :error-messages="{ required: 'This field is required' }"
        />
      </lf-field>
    </div>
    <div class="py-4.5 px-6 bg-g ray-50 flex justify-end">
      <lf-button
        type="secondary-ghost"
        class="mr-4"
        @click="isModalOpen = false"
      >
        Cancel
      </lf-button>
      <lf-button
        :disabled="$v.$invalid"
        @click="submit()"
      >
        {{ isEdit ? 'Update' : 'Add' }} category
      </lf-button>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">
import {
  computed, onMounted, reactive,
} from 'vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfField from '@/ui-kit/field/Field.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import type { Category } from '@/modules/admin/modules/categories/types/Category';

const props = defineProps<{
  modelValue: boolean;
  category?: Category,
  categoryGroupId?: string;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'add', value: Category): void;
  (e: 'update', value: Category): void;
}>();

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
});

const isEdit = computed(() => !!props.category);

const form = reactive({
  name: '',
});

const rules = {
  name: {
    required,
  },
};

const $v = useVuelidate(rules, form, {
  $stopPropagation: true,
});

const fillForm = () => {
  form.name = props.category?.name || '';
};

const submit = () => {
  if ($v.value.$invalid) {
    $v.value.$touch();
    return;
  }

  if (isEdit.value) {
    emit('update', {
      ...props.category,
      ...form,
    } as Category);
  } else {
    emit('add', {
      ...form,
    } as Category);
  }
  isModalOpen.value = false;
};

onMounted(() => {
  fillForm();
});
</script>

<script lang="ts">
export default {
  name: 'LfCategoryForm',
};
</script>
