<template>
  <lf-drawer v-model="isDrawerOpen" class="flex flex-col">
    <header class="px-6 pb-6 pt-4 flex justify-between items-center">
      <h2 class="text-lg font-semibold text-gray-900">
        {{ isEdit ? 'Edit' : 'Add' }} category group
      </h2>
      <lf-button
        type="secondary-ghost"
        icon-only
        @click="isDrawerOpen = false"
      >
        <lf-icon
          name="xmark"
          class="text-gray-500"
        />
      </lf-button>
    </header>

    <div class="px-6 pb-8">
      <lf-field label-text="Category group name" :required="true" class="mb-6">
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
      <lf-field label-text="Type" :required="true">
        <div class="flex items-center pt-2">
          <lf-radio v-model="form.type" value="vertical" class="mr-4">
            Industry
          </lf-radio>
          <lf-radio v-model="form.type" value="horizontal" class="mr-4">
            Stack
          </lf-radio>
        </div>
        <lf-field-messages
          :validation="$v.type"
          :error-messages="{ required: 'This field is required' }"
        />
      </lf-field>
    </div>

    <div class="flex-grow">
      <div class="px-6 py-2 bg-gray-100 flex justify-between items-center">
        <p class="text-gray-400 text-medium font-semibold">
          Categories
        </p>
        <lf-button
          type="primary-ghost"
          size="small"
          @click="isCategoryFormOpen = true; selectedCategory = null;"
        >
          <lf-icon
            name="plus"
          />
          Add category
        </lf-button>
      </div>

      <div v-if="categories.length > 0" class="py-3 px-6">
        <article
          v-for="(category, ci) of categories"
          :key="category.id"
          class="border-t border-gray-100 first:border-none flex justify-between items-center py-2.5"
        >
          <p class="text-medium">
            {{ category.name }}
          </p>
          <div class="flex items-center gap-2">
            <lf-button
              type="secondary-ghost"
              icon-only
              @click="isCategoryFormOpen = true; selectedCategory = category;"
            >
              <lf-icon
                name="edit"
                class="text-gray-500"
              />
            </lf-button>
            <lf-button
              type="secondary-ghost"
              icon-only
              @click="deleteCategory(ci)"
            >
              <lf-icon
                name="trash-can"
                class="text-gray-500"
              />
            </lf-button>
          </div>
        </article>
      </div>
      <div v-else class="pt-16 px-6 flex flex-col items-center">
        <lf-icon
          name="folder"
          :size="64"
          class="text-gray-300"
        />
        <h6 class="text-center pt-6">
          No categories yet
        </h6>
        <p class="text-center pt-3 text-gray-500 text-small">
          Start adding categories into this category group
        </p>
      </div>
    </div>

    <div class="border-t border-gray-100 px-6 py-4 flex justify-end items-center">
      <lf-button
        type="secondary-ghost"
        class="mr-4"
        @click="cancel()"
      >
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        :disabled="$v.$invalid"
        :loading="isSending"
        @click="submit()"
      >
        {{ isEdit ? 'Update' : 'Add' }} category group
      </lf-button>
    </div>
  </lf-drawer>
  <lf-category-form
    v-if="isCategoryFormOpen"
    v-model="isCategoryFormOpen"
    :category="selectedCategory || undefined"
    :category-group-id="props.categoryGroup?.id"
    @add="categories.push($event)"
    @update="updateCategory($event)"
  />
</template>

<script setup lang="ts">
import LfDrawer from '@/ui-kit/drawer/Drawer.vue';
import {
  computed, onMounted, reactive, ref,
} from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfField from '@/ui-kit/field/Field.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import LfRadio from '@/ui-kit/radio/Radio.vue';
import LfCategoryForm from '@/modules/admin/modules/categories/components/form/category-form.vue';
import { CategoryGroupService } from '@/modules/admin/modules/categories/services/category-group.service';
import type { CategoryGroup, CategoryGroupType } from '@/modules/admin/modules/categories/types/CategoryGroup';
import { ToastStore } from '@/shared/message/notification';
import { CategoryService } from '@/modules/admin/modules/categories/services/category.service';
import type { Category } from '@/modules/admin/modules/categories/types/Category';

const props = defineProps<{
  modelValue: boolean;
  categoryGroup?: CategoryGroup,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'reload'): void;
}>();

const categories = ref(props.categoryGroup?.categories || []);

const isDrawerOpen = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
});

const isCategoryFormOpen = ref(false);
const selectedCategory = ref<Category | null>(null);

const isSending = ref(false);
const isEdit = computed(() => !!props.categoryGroup);

const form = reactive({
  name: '',
  type: '',
});

const rules = {
  name: {
    required,
  },
  type: {
    required,
  },
};

const $v = useVuelidate(rules, form);

const reset = () => {
  form.name = '';
  form.type = '';
  $v.value.$reset();
};

const fillForm = () => {
  form.name = props.categoryGroup?.name || '';
  form.type = props.categoryGroup?.type || '';
};

const submit = () => {
  if ($v.value.$invalid) {
    $v.value.$touch();
    return;
  }
  const call = isEdit.value
    ? CategoryGroupService.update(props.categoryGroup!.id, {
      name: form.name,
      type: form.type as CategoryGroupType,
      categories: categories.value,
    })
    : CategoryGroupService.create({
      name: form.name,
      type: form.type as CategoryGroupType,
      categories: categories.value,
    });
  isSending.value = true;

  call
    .then(() => {
      ToastStore.success(`${isEdit.value ? 'Updated' : 'Created'} category group`);
      reset();
      emit('reload');
      isDrawerOpen.value = false;
    })
    .catch(() => {
      ToastStore.error(`Error occurred while ${isEdit.value ? 'updating' : 'creating'} category group`);
    })
    .finally(() => {
      isSending.value = false;
    });
};

const deleteCategory = (index: number) => {
  categories.value.splice(index, 1);
};

const updateCategory = (category: Category) => {
  categories.value = categories.value.map((c) => (c.id === category.id ? category : c));
};

const cancel = () => {
  const categoriesWithoutGroup = categories.value.filter((category) => !category.categoryGroupId).map((category) => category.id);
  CategoryService.deleteBulk(categoriesWithoutGroup);
  isDrawerOpen.value = false;
};

onMounted(() => {
  fillForm();
});
</script>

<script lang="ts">
export default {
  name: 'LfCategoryGroupForm',
};
</script>
