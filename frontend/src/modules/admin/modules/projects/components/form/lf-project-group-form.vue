<template>
  <app-drawer
    v-model="model"
    has-border
    :title="isEditForm ? 'Edit project group' : 'Add project group'"
    :size="480"
    @close="model = false"
  >
    <template #content>
      <div
        v-if="isLoading"
        v-loading="isLoading"
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
            placeholder="E.g. Cloud Native Computing Foundation"
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
          }"
        >
          <el-input v-model="form.slug" placeholder="E.g. cncf" />
        </app-form-item>

        <!-- Source ID -->
        <app-form-item
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

        <!-- Logo URL -->
        <app-form-item
          label="Logo URL"
          class="mb-6"
          required
          :validation="$v.url"
          :error-messages="{
            required: 'Logo URL is required',
          }"
        >
          <el-input v-model="form.url" />
        </app-form-item>

        <!-- Status -->
        <app-form-item
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
            <template v-if="form.status" #prefix>
              <span
                class="w-1.5 h-1.5 rounded-full mr-1"
                :class="
                  statusOptions.find((o) => o.value === form.status)?.color
                "
              />
            </template>
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
        {{ isEditForm ? "Update" : "Add project group" }}
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
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfButton from '@/ui-kit/button/Button.vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { segmentService } from '@/modules/lf/segments/segments.service';
import { TanstackKey } from '@/shared/types/tanstack';
import type { ProjectGroup } from '@/modules/lf/segments/types/Segments';
import type { AxiosError } from 'axios';
import { getAxiosErrorMessage } from '@/shared/helpers/error-message.helper';

import { ToastStore } from '@/shared/message/notification';

const props = defineProps<{
  modelValue: boolean;
  id: string | null;
}>();

const emit = defineEmits<{(e: 'update:modelValue', v: boolean): void;
  (e: 'onProjectGroupEdited'): void;
}>();

const { trackEvent } = useProductTracking();

const submitLoading = ref(false);
const form = reactive({
  name: '',
  slug: '',
  sourceId: '',
  url: '',
  status: '',
});

const rules = {
  name: {
    required,
    maxLength,
  },
  slug: { required },
  sourceId: { required },
  url: { required },
  status: { required },
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

const fillForm = (record?: ProjectGroup) => {
  if (record) {
    Object.assign(form, record);
  }

  formSnapshot();
};

const {
  isLoading,
  isSuccess,
  data,
} = useQuery(
  {
    queryKey: [TanstackKey.ADMIN_PROJECT_GROUPS, props.id],
    queryFn: () => {
      if (!props.id) {
        return Promise.resolve(null);
      }
      return segmentService.getSegmentById(props.id);
    },
    enabled: !!props.id,
  },
);

watch(data, () => {
  if (isSuccess.value && data.value) {
    fillForm(data.value as ProjectGroup);
  }
}, { immediate: true });

onMounted(() => {
  if (!props.id) {
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
      key: FeatureEventKey.EDIT_PROJECT_GROUP,
      type: EventType.FEATURE,
    });

    updateMutation.mutate({
      id: props.id!,
      form: form as ProjectGroup,
    });
  } else {
    trackEvent({
      key: FeatureEventKey.ADD_PROJECT_GROUP,
      type: EventType.FEATURE,
    });

    createMutation.mutate(form);
  }
};

const queryClient = useQueryClient();
const onSuccess = () => {
  submitLoading.value = false;
  model.value = false;
  queryClient.invalidateQueries({
    queryKey: [TanstackKey.ADMIN_PROJECT_GROUPS],
  });
  ToastStore.success(`Project Group ${props.id ? 'updated' : 'created'} successfully`);
};

const onError = (err: AxiosError) => {
  ToastStore.error(getAxiosErrorMessage(err, `Something went wrong while ${props.id ? 'updating' : 'creating'} the project group`));
};

const updateMutation = useMutation({
  mutationFn: ({ id, form }: { id: string; form: ProjectGroup }) => segmentService.updateSegment(
    id,
    form,
  ),
  onSuccess: () => {
    onSuccess();
    emit('onProjectGroupEdited');
  },
  onError,
});

const createMutation = useMutation({
  mutationFn: (form: any) => segmentService.createProjectGroup(form),
  onSuccess,
  onError,

});
</script>

<script lang="ts">
export default {
  name: 'AppLfProjectGroupForm',
};
</script>
