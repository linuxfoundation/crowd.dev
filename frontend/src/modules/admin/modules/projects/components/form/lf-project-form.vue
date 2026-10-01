<template>
  <app-drawer
    v-model="model"
    has-border
    :title="isEditForm ? 'Edit project' : 'Add project'"
    :size="600"
    @close="model = false"
  >
    <template #content>
      <div
        v-if="isLoading"
        v-loading="isLoading"
        class="app-page-spinner h-16 !relative !min-h-5"
      />
      <div v-else>
        <lf-field class="mb-6" :required="true">
          <div class="flex items-center pt-2">
            <lf-radio v-model="form.type" value="LF" class="mr-4">
              <div class="flex items-center">
                <lf-svg name="lfx" class="w-5 h-4 mr-1 flex items-center" />
                Linux Foundation project
              </div>
            </lf-radio>
            <lf-radio v-model="form.type" value="nonLF" class="mr-4">
              Non-Linux Foundation project
            </lf-radio>
          </div>
        </lf-field>
        <!-- Name -->
        <app-form-item
          label="Name"
          class="mb-6"
          :required="true"
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
          :required="true"
          :validation="$v.slug"
          :error-messages="{
            required: 'Slug is required',
            mustNotStartWithSpecial: 'Cannot start with %, #, or !',
          }"
        >
          <el-input
            v-model="form.slug"
            placeholder="E.g. kubernetes"
            @blur="$v.slug.$touch"
          />
        </app-form-item>

        <!-- Source ID -->
        <app-form-item
          v-if="form.type === ProjectType.LF"
          label="Source ID"
          class="mb-6"
          :required="true"
          :validation="$v.sourceId"
          :error-messages="{
            required: 'Source ID is required',
          }"
        >
          <el-input v-model="form.sourceId" />
        </app-form-item>

        <!-- Status -->
        <app-form-item
          v-if="form.type === ProjectType.LF"
          label="Status"
          class="mb-6"
          :required="true"
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
        :disabled="!hasFormChanged || $v.$invalid || isLoading"
        @click="onSubmit"
      >
        {{ isEditForm ? 'Update' : 'Add project' }}
      </lf-button>
    </template>
  </app-drawer>
</template>

<script setup lang="ts">
import formChangeDetector from '@/shared/form/form-change';
import useVuelidate from '@vuelidate/core';
import { required, maxLength } from '@vuelidate/validators';
import {
  computed, onMounted, reactive, ref, watch,
} from 'vue';
import AppFormItem from '@/shared/form/form-item.vue';
import statusOptions from '@/modules/lf/config/status';
import { useRoute } from 'vue-router';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfButton from '@/ui-kit/button/Button.vue';
import type { Project, ProjectRequest } from '@/modules/lf/segments/types/Segments';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { TanstackKey } from '@/shared/types/tanstack';
import { segmentService } from '@/modules/lf/segments/segments.service';

import { ToastStore } from '@/shared/message/notification';
import LfField from '@/ui-kit/field/Field.vue';
import LfRadio from '@/ui-kit/radio/Radio.vue';
import LfSvg from '@/shared/svg/svg.vue';
import type { AxiosError } from 'axios';

const enum ProjectType {
  LF = 'LF',
  NON_LF = 'nonLF',
}

const emit = defineEmits<{(e: 'update:modelValue', v: boolean): void;
  (e: 'onSuccess'): void;
}>();

const props = defineProps<{
  modelValue: boolean;
  id?: string | null;
  parentSlug: string;
  isLFProject?: boolean;
}>();

const route = useRoute();

const { trackEvent } = useProductTracking();

const submitLoading = ref(false);
const form = reactive({
  name: '',
  slug: '',
  sourceId: '',
  status: '',
  type: props.isLFProject ? ProjectType.LF : ProjectType.NON_LF,
  parentSlug: props.parentSlug,
});

// Custom prefix validator
const mustNotStartWithSpecial = (value: string) => {
  if (!value) return true;
  return !/^[%#!]/.test(value);
};

const rules = computed(() => ({
  name: {
    required,
    maxLength: maxLength(50),
  },
  slug:
    form.type === ProjectType.LF
      ? { required }
      : { required, mustNotStartWithSpecial },
  type: { required },
  sourceId: form.type === ProjectType.LF ? { required } : {},
  status: form.type === ProjectType.LF ? { required } : {},
  parentSlug: { required },
}));

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

const fillForm = (record?: Project) => {
  if (record) {
    Object.assign(form, record);
    form.type = record.isLF ? ProjectType.LF : ProjectType.NON_LF;
  }

  formSnapshot();
};

const { isLoading, isSuccess, data } = useQuery({
  queryKey: [TanstackKey.ADMIN_PROJECT_GROUPS, props.id],
  queryFn: () => {
    if (!props.id) {
      return Promise.resolve(null);
    }
    return segmentService.getSegmentById(props.id);
  },
  enabled: !!props.id,
});

watch(
  data,
  () => {
    if (isSuccess.value && data.value) {
      fillForm(data.value as Project);
    }
  },
  { immediate: true },
);

onMounted(() => {
  if (!props.id) {
    fillForm();
  }
});

const queryClient = useQueryClient();
const onSuccess = () => {
  submitLoading.value = false;
  model.value = false;
  queryClient.invalidateQueries({
    queryKey: [TanstackKey.ADMIN_PROJECT_GROUPS],
  });
  ToastStore.success(`Project ${props.id ? 'updated' : 'created'} successfully`);
  emit('onSuccess');
};

const onError = (error: AxiosError) => {
  ToastStore.error(
    error?.response?.data ? error.response.data
      : `Something went wrong while ${props.id ? 'updating' : 'creating'} the project`,
  );
};

const updateMutation = useMutation({
  mutationFn: ({ id, project }: { id: string; project: ProjectRequest }) => segmentService.updateSegment(id, project),
  onSuccess,
  onError,
});

const createMutation = useMutation({
  mutationFn: (req: { project: ProjectRequest; segments: string[] }) => segmentService.createProject(req),
  onSuccess,
  onError,
});

const onCancel = () => {
  model.value = false;
};

const onSubmit = () => {
  submitLoading.value = true;

  if (isEditForm.value) {
    trackEvent({
      key: FeatureEventKey.EDIT_PROJECT,
      type: EventType.FEATURE,
    });
    updateMutation.mutate({
      id: props.id!,
      project: buildRequest(),
    });
  } else {
    trackEvent({
      key: FeatureEventKey.ADD_PROJECT,
      type: EventType.FEATURE,
    });

    createMutation.mutate({
      project: buildRequest(),
      segments: [route.params.id as string],
    });
  }
};

const buildRequest = (): ProjectRequest => ({
  name: form.name,
  slug: form.slug,
  sourceId: form.sourceId,
  status: form.status,
  isLF: form.type === ProjectType.LF,
  parentSlug: form.parentSlug,
});
</script>

<script lang="ts">
export default {
  name: 'AppLfProjectForm',
};
</script>
