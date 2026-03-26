<template>
  <app-drawer
    v-model="model"
    :title="isEditForm ? 'Edit collection' : 'Add collection'"
    :size="600"
    @close="onCancel"
  >
    <template #content>
      <div
        v-if="loading"
        v-loading="loading"
        class="app-page-spinner h-16 !relative !min-h-5"
      />
      <div v-else>
        <lf-tabs v-model="activeTab" :fragment="false">
          <lf-tab name="details">
            Details
          </lf-tab>
          <lf-tab name="projects">
            Projects
          </lf-tab>
        </lf-tabs>
        <div class="pt-6 border-t border-gray-100">
          <div class="tab-content">
            <div v-if="activeTab === 'details'">
              <!-- Collection name -->
              <article class="mb-6">
                <lf-field label-text="Collection name" :required="true">
                  <lf-input
                    v-model="form.name"
                    class="h-10"
                    :invalid="$v.name.$invalid && $v.name.$dirty"
                    @blur="$v.name.$touch()"
                    @change="$v.name.$touch()"
                  />
                  <lf-field-messages
                    :validation="$v.name"
                    :error-messages="{ required: 'This field is required' }"
                  />
                </lf-field>
              </article>

              <!-- Description -->
              <article class="mb-6">
                <lf-field label-text="Description" :required="true">
                  <lf-textarea
                    v-model="form.description"
                    :invalid="$v.description.$invalid && $v.description.$dirty"
                    @blur="$v.description.$touch()"
                    @change="$v.description.$touch()"
                  />
                  <lf-field-messages
                    :validation="$v.description"
                    :error-messages="{ required: 'This field is required' }"
                  />
                </lf-field>
              </article>

              <!-- Category -->
              <article class="mb-5">
                <lf-field label-text="Category">
                  <div class="flex">
                    <div class="w-1/4">
                      <el-select
                        v-model="form.type"
                        placeholder="Select type"
                        class="w-full type-select"
                        clearable
                      >
                        <el-option label="Industry" value="vertical" />
                        <el-option label="Stack" value="horizontal" />
                      </el-select>
                    </div>
                    <div class="w-3/4">
                      <el-select
                        v-model="form.categoryId"
                        placeholder="Select type"
                        class="w-full category-select"
                        filterable
                        clearable
                        remote
                        :disabled="!form.type?.length"
                        :remote-method="fetchCategories"
                        @clear="
                          () => {
                            form.categoryId = null;
                          }
                        "
                      >
                        <el-option
                          v-if="collection"
                          :label="props.collection?.category?.name"
                          :value="props.collection?.categoryId"
                          class="!px-3 !hidden"
                        />
                        <template v-for="group of categories" :key="group.id">
                          <div
                            class="px-3 pt-1 text-xs font-semibold text-gray-400"
                          >
                            {{ group.name }}
                          </div>
                          <el-option
                            v-for="option of group.categories"
                            :key="option.id"
                            :label="option.name"
                            :value="option.id"
                            class="!px-3"
                          />
                        </template>
                      </el-select>
                    </div>
                  </div>
                  <lf-field-messages
                    :validation="$v.category"
                    :error-messages="{ required: 'This field is required' }"
                  />
                </lf-field>
              </article>

              <!-- Appearance section -->
              <div class="mb-6">
                <div class="flex items-center gap-3">
                  <h6 class="text-sm font-primary font-semibold text-gray-500 mb-4  pb-2">
                    Appearance
                  </h6>
                  <div class="border-b border-gray-100 w-full" />
                </div>

                <!-- Card image URL -->
                <article class="mb-6">
                  <lf-field label-text="Card image URL" :required="true">
                    <lf-input
                      v-model="form.imageUrl"
                      class="h-10"
                      placeholder="https://example.com/card-image.png"
                      :invalid="$v.imageUrl.$invalid && $v.imageUrl.$dirty"
                      @blur="$v.imageUrl.$touch()"
                      @change="$v.imageUrl.$touch()"
                    />
                    <lf-field-messages
                      :validation="$v.imageUrl"
                      :error-messages="{ url: 'Please enter a valid URL' }"
                    />
                    <span class="text-2xs text-gray-400">Recommended image size: 800×240px</span>
                  </lf-field>
                </article>

                <!-- Collection logo URL -->
                <article class="mb-6">
                  <lf-field label-text="Collection logo URL" :required="true">
                    <lf-input
                      v-model="form.logoUrl"
                      class="h-10"
                      placeholder="https://example.com/logo.png"
                      :invalid="$v.logoUrl.$invalid && $v.logoUrl.$dirty"
                      @blur="$v.logoUrl.$touch()"
                      @change="$v.logoUrl.$touch()"
                    />
                    <lf-field-messages
                      :validation="$v.logoUrl"
                      :error-messages="{ url: 'Please enter a valid URL' }"
                    />
                    <span class="text-2xs text-gray-400">Recommended image size: 400×400px</span>
                  </lf-field>
                </article>

                <!-- Collection color -->
                <article class="mb-6">
                  <lf-field label-text="Collection color">
                    <div class="flex items-center gap-2">
                      <lf-input
                        v-model="form.color"
                        class="h-10 flex-grow"
                        placeholder="Enter HEX code"
                      >
                        <template #prefix>
                          <div
                            v-if="form.color"
                            class="w-6 h-6 rounded-full flex-shrink-0 border border-gray-200"
                            :style="{ backgroundColor: form.color || '#009AFF' }"
                          />
                        </template>
                      </lf-input>
                    </div>
                    <span class="text-2xs text-gray-400">Leave blank to use the default color #009AFF</span>
                  </lf-field>
                </article>
              </div>
            </div>
            <lf-collection-add-projects-tab
              v-if="activeTab === 'projects'"
              :form="form"
            />
          </div>
        </div>
      </div>
    </template>
    <template #footer>
      <lf-button type="secondary-ghost" @click="onCancel">
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        :disabled="!hasFormChanged || $v.$invalid || loading"
        @click="onSubmit"
      >
        {{ isEditForm ? "Update" : "Add collection" }}
      </lf-button>
    </template>
  </app-drawer>
</template>
<script setup lang="ts">
import formChangeDetector from '@/shared/form/form-change';
import useVuelidate from '@vuelidate/core';
import { required, maxLength, url } from '@vuelidate/validators';
import {
  computed, onMounted, reactive, ref, watch,
} from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfTextarea from '@/ui-kit/textarea/Textarea.vue';
import LfField from '@/ui-kit/field/Field.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';

import { ToastStore } from '@/shared/message/notification';
import { CategoryGroup } from '@/modules/admin/modules/categories/types/CategoryGroup';
import { CategoryService } from '@/modules/admin/modules/categories/services/category.service';
import AppDrawer from '@/shared/drawer/drawer.vue';
import { useMutation, useQueryClient } from '@tanstack/vue-query';
import { TanstackKey } from '@/shared/types/tanstack';
import LfCollectionAddProjectsTab from './lf-collection-add-projects-tab.vue';
import {
  CollectionFormModel,
  CollectionModel,
  CollectionRequest,
} from '../models/collection.model';
import { COLLECTIONS_SERVICE } from '../services/collections.service';

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'onCollectionEdited'): void;
  (e: 'onCollectionCreated'): void;
}>();

const props = defineProps<{
  modelValue: boolean;
  collection?: CollectionModel;
}>();

const activeTab = ref('details');
const loading = ref(false);
const form = reactive<CollectionFormModel>({
  name: '',
  description: '',
  type: '',
  categoryId: null,
  logoUrl: '',
  imageUrl: '',
  color: '',
  projects: [],
  starred: false,
});

const rules = {
  name: {
    required,
    maxLength,
  },
  description: { required: (value: string) => value.trim().length },
  imageUrl: { url },
  logoUrl: { url },
  projects: { required: (value: any) => value.length > 0 },
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

const isEditForm = computed(() => !!props.collection?.id);

const fillForm = (record?: CollectionModel) => {
  if (record) {
    Object.assign(form, record);
    form.type = record.category?.categoryGroupType;
    form.categoryId = record.categoryId || null;
    form.logoUrl = record.logoUrl || '';
    form.imageUrl = record.imageUrl || '';
    form.color = record.color || '';
  }

  formSnapshot();
};

onMounted(() => {
  if (isEditForm.value) {
    loading.value = true;
    fillForm(props.collection);
    loading.value = false;
  } else {
    fillForm();
  }
});

const onCancel = () => {
  model.value = false;
};

const onSubmit = () => {
  const request: CollectionRequest = {
    name: form.name,
    description: form.description,
    logoUrl: form.logoUrl || undefined,
    imageUrl: form.imageUrl || undefined,
    color: form.color || undefined,
    projects: form.projects.map((project: any) => ({
      id: project.id,
      starred: project?.starred || false,
    })),
    starred: !!form.starred,
    categoryId: form.categoryId,
    slug: form.name.toLowerCase().replace(/ /g, '-'),
  };
  if (isEditForm.value) {
    updateMutation.mutate({
      id: props.collection!.id,
      form: request,
    });
  } else {
    createMutation.mutate(request);
  }
};

const queryClient = useQueryClient();
const onSuccess = () => {
  queryClient.invalidateQueries({
    queryKey: [TanstackKey.ADMIN_COLLECTIONS],
  });
  ToastStore.closeAll();
  ToastStore.success(
    `Collection ${isEditForm.value ? 'updated' : 'created'} successfully`,
  );
  if (isEditForm.value) {
    emit('onCollectionEdited');
  } else {
    emit('onCollectionCreated');
  }
};

const onError = () => {
  ToastStore.closeAll();
  ToastStore.error(
    `Something went wrong while ${isEditForm.value ? 'updating' : 'creating'} the collection`,
  );
};

const createMutation = useMutation({
  mutationFn: (form: CollectionRequest) => COLLECTIONS_SERVICE.create(form),
  onSuccess,
  onError,
});

const updateMutation = useMutation({
  mutationFn: ({ id, form }: { id: string; form: CollectionRequest }) => COLLECTIONS_SERVICE.update(id, form),
  onSuccess,
  onError,
});

const categories = ref<CategoryGroup[]>([]);

const fetchCategories = (query: string) => {
  CategoryService.list({
    offset: 0,
    limit: 20,
    query,
    groupType: form.type || undefined,
  }).then((res) => {
    form.categoryId = !form.categoryId
      ? props.collection?.categoryId || null
      : form.categoryId;
    categories.value = res.rows;
  });
};

watch(
  () => form.type,
  () => {
    form.categoryId = null;
    fetchCategories('');
  },
);
</script>

<script lang="ts">
export default {
  name: 'LfCollectionAdd',
};
</script>

<style lang="scss">
.type-select {
  .el-input__wrapper {
    @apply rounded-r-none border-r-0 #{!important};
  }
}

.category-select {
  .el-input__wrapper {
    @apply rounded-l-none #{!important};
  }
}
</style>
