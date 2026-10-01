<template>
  <app-drawer
    v-model="model"
    has-border
    :title="isEditForm ? 'Edit sub-project' : 'Add sub-project'"
    :size="480"
    @close="model = false"
  >
    <template #content>
      <div
        v-if="loading"
        v-loading="loading"
        class="app-page-spinner h-16 !relative !min-h-5"
      />
      <div v-else>
        <!-- Name -->
        <app-form-item
          label="Name"
          class="mb-6"
          required
          :validation="$v.name"
          :error-messages="{
            required: 'Name is required',
          }"
        >
          <el-input
            v-model="form.name"
            maxlength="100"
            show-word-limit
            placeholder="E.g. Kubernetes"
          />
        </app-form-item>

        <!-- Slug -->
        <app-form-item
          label="Slug"
          class="mb-6"
          required
          :validation="$v.slug"
          :error-messages="{
            required: 'Slug is required',
            mustNotStartWithSpecial: 'Cannot start with %, #, or !',
          }"
        >
          <el-input v-model="form.slug" placeholder="E.g. kubernetes" @blur="$v.slug.$touch()" />
        </app-form-item>

        <!-- Source ID -->
        <app-form-item
          v-if="isLFProject"
          label="Source ID"
          class="mb-6"
          required
          :validation="$v.sourceId"
          :error-messages="{
            required: 'Source ID is required',
          }"
        >
          <el-input v-model="form.sourceId" />
        </app-form-item>

        <!-- Status -->
        <app-form-item
          v-if="isLFProject"
          label="Status"
          class="mb-6"
          required
          :validation="$v.status"
          :error-messages="{
            required: 'Status is required',
          }"
        >
          <el-select
            v-model="form.status"
            placeholder="Select option"
            class="w-full"
            @blur="$v.status.$touch"
          >
            <el-option
              v-for="status of statusOptions"
              :key="status.value"
              :label="status.label"
              :value="status.value"
            >
              <div class="flex items-center gap-3">
                <span
                  class="w-1.5 h-1.5 rounded-full"
                  :class="status.color"
                />{{ status.label }}
              </div>
            </el-option>
          </el-select>
        </app-form-item>
      </div>
    </template>
    <template #footer>
      <lf-button
        type="secondary-gray"
        size="medium"
        class="mr-4"
        @click="onCancel"
      >
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        size="medium"
        :disabled="!hasFormChanged || $v.$invalid || loading"
        @click="onSubmit"
      >
        {{ isEditForm ? 'Update' : 'Add sub-project' }}
      </lf-button>
    </template>
  </app-drawer>
</template>

<script setup lang="ts">
import formChangeDetector from '@/shared/form/form-change';
import useVuelidate from '@vuelidate/core';
import { required, maxLength } from '@vuelidate/validators';
import {
  computed, onMounted, reactive, ref,
} from 'vue';
import AppFormItem from '@/shared/form/form-item.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import statusOptions from '@/modules/lf/config/status';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfButton from '@/ui-kit/button/Button.vue';
import type { SubProject } from '@/modules/lf/segments/types/Segments';

const props = withDefaults(
  defineProps<{
    modelValue: boolean;
    id?: string | null;
    isLFProject?: boolean;
    parentSlug?: string | null;
    parentId?: string | null;
    grandparentSlug?: string | null;
    grandparentId?: string | null;
  }>(),
  {
    id: null,
    isLFProject: true,
    parentSlug: null,
    parentId: null,
    grandparentSlug: null,
    grandparentId: null,
  },
);

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void, (e: 'onSuccess'): void
}>();
const { trackEvent } = useProductTracking();

const lsSegmentsStore = useLfSegmentsStore();
const { createSubProject, updateSubProject, findSubProject } = lsSegmentsStore;

const loading = ref(false);
const submitLoading = ref(false);
const form = reactive({
  name: '',
  slug: '',
  sourceId: '',
  status: '',
  isLF: props.isLFProject,
  parentSlug: props.parentSlug,
  grandparentSlug: props.grandparentSlug,
});

// Custom prefix validator
const mustNotStartWithSpecial = (value: string) => {
  if (!value) return true;
  return !/^[%#!]/.test(value);
};

const rules = {
  name: {
    required,
    maxLength: maxLength(50),
  },
  slug: props.isLFProject
    ? { required }
    : { required, mustNotStartWithSpecial },
  sourceId: props.isLFProject ? { required } : {},
  status: props.isLFProject ? { required } : {},
  parentSlug: { required },
  grandparentSlug: { required },
};

const $v = useVuelidate(rules, form);

const { hasFormChanged, formSnapshot } = formChangeDetector(form);

const model = computed({
  get() {
    return props.modelValue;
  },
  set(v) {
    emit('update:modelValue', v);
  },
});

const isEditForm = computed(() => !!props.id);

const fillForm = (record?: SubProject) => {
  if (record) {
    Object.assign(form, record);
  }

  formSnapshot();
};

onMounted(() => {
  if (props.id) {
    loading.value = true;

    findSubProject(props.id)
      .then((response) => {
        fillForm(response);
      })
      .finally(() => {
        loading.value = false;
      });
  } else {
    fillForm();
  }
});

const onCancel = () => {
  model.value = false;
};

const onSubmit = () => {
  submitLoading.value = true;

  if (isEditForm.value) {
    trackEvent({
      key: FeatureEventKey.EDIT_SUB_PROJECT,
      type: EventType.FEATURE,
    });

    updateSubProject(props.id, form).then(() => emit('onSuccess')).finally(() => {
      submitLoading.value = false;
      model.value = false;
    });
  } else {
    trackEvent({
      key: FeatureEventKey.ADD_SUB_PROJECT,
      type: EventType.FEATURE,
    });

    createSubProject({
      ...form,
      segments: [props.parentId, props.grandparentId],
    }).then(() => emit('onSuccess')).finally(() => {
      submitLoading.value = false;
      model.value = false;
    });
  }
};
</script>

<script lang="ts">
export default {
  name: 'AppLfSubProjectForm',
};
</script>
