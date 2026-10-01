<template>
  <app-drawer v-model="isDrawerOpen" :title="title" size="600px">
    <template #content>
      <div class="-mx-6 -mt-4">
        <!-- Share with workspace -->
        <div class="border-t border-gray-200" />
        <section v-if="hasPermission(LfPermission.customViewsTenantManage)" class="border-b border-gray-200 bg-gray-50 py-4 px-6">
          <div class="flex justify-between items-center">
            <div>
              <h6 class="text-sm font-medium leading-5">
                Shared with workspace
              </h6>
              <p class="text-2xs leading-4.5 text-gray-500">
                If enabled, the view will be shared with all users from this workspace.
              </p>
            </div>
            <div>
              <lf-switch
                v-model="form.shared"
                :disabled="!!isEdit || !hasPermission(LfPermission.customViewsTenantManage)"
                class="ml-4"
              />
            </div>
          </div>
        </section>

        <!-- Name and setting -->
        <section class="p-6">
          <app-form-item
            :validation="$v.name"
            :error-messages="{
              required: 'This field is required',
            }"
            label="View name"
            :required="true"
            class="mb-0"
          >
            <el-input
              v-model="form.name"
              @blur="$v.name.$touch"
              @change="$v.name.$touch"
            />
          </app-form-item>
        </section>

        <!-- Settings -->
        <section v-if="Object.keys(settings).length > 0" class="px-6">
          <div class="border-b border-gray-200 pb-1">
            <div v-for="(setting, settingsKey) in settings" :key="settingsKey" class="pb-3">
              <component :is="setting.settingsComponent" v-model="form.settings[settingsKey]" :settings="form.settings" />
            </div>
          </div>
        </section>

        <!-- Filters -->
        <section class="px-6">
          <div class="py-6 flex items-center justify-between">
            <h6 class="text-base font-semibold">
              Filters
            </h6>
            <el-dropdown v-if="filterList.length > 0" placement="bottom-end">
              <p class="text-xs leading-5 font-medium text-gray-900">
                Matching {{ form.relation === 'and' ? 'all' : 'any' }} <lf-icon name="chevron-down" />
              </p>
              <template #dropdown>
                <el-dropdown-item
                  class="flex items-center justify-between"
                  :class="{ 'bg-primary-50': form.relation === 'and' }"
                  @click="form.relation = 'and'"
                >
                  Matching all
                  <lf-icon
                    name="check"
                    :class="form.relation === 'and' ? 'opacity-100' : 'opacity-0'"
                    class="!text-primary-500 !mr-0 ml-1"
                  />
                </el-dropdown-item>
                <el-dropdown-item
                  class="flex items-center justify-between"
                  :class="{ 'bg-primary-50': form.relation === 'or' }"
                  @click="form.relation = 'or'"
                >
                  Matching any
                  <lf-icon
                    name="check"
                    :class="form.relation === 'or' ? 'opacity-100' : 'opacity-0'"
                    class="!text-primary-500 !mr-0 ml-1"
                  />
                </el-dropdown-item>
              </template>
            </el-dropdown>
          </div>
          <div>
            <div v-for="filter of filterList" :key="filter" class="flex items-center mb-3">
              <lf-filter-item
                v-model="form.filters[filter]"
                v-model:open="openedFilter"
                :config="allFilters[filter]"
                :hide-remove="true"
                class="flex-grow"
                chip-classes="w-full !h-10"
              />
              <div
                class="ml-2 h-10 w-10 flex items-center justify-center cursor-pointer"
                @click="removeFilter(filter)"
              >
                <lf-icon name="trash-can" :size="20" class="text-gray-400 flex items-center" />
              </div>
            </div>
          </div>
          <div
            class="flex pb-10 border-b border-gray-200 pt-1"
          >
            <el-dropdown placement="bottom-start" trigger="click" popper-class="!p-0">
              <p class="text-xs font-medium leading-5 text-primary-500">
                + Add filter
              </p>
              <template #dropdown>
                <div class="-m-2">
                  <div class="border-b border-gray-100 p-2">
                    <el-input
                      v-model="dropdownSearch"
                      placeholder="Search..."
                      class="filter-dropdown-search"
                      data-qa="filter-list-search"
                    >
                      <template #prefix>
                        <lf-icon name="magnifying-glass" />
                      </template>
                    </el-input>
                  </div>
                  <div class="m-2 max-h-56 overflow-auto">
                    <el-dropdown-item
                      v-for="[key, filterConfig] of filteredFilters"
                      :key="key"
                      :disabled="filterList.includes(key)"
                      @click="addFilter(key)"
                    >
                      {{ filterConfig.label }}
                    </el-dropdown-item>
                  </div>
                </div>
              </template>
            </el-dropdown>
          </div>
        </section>

        <!-- Filters -->
        <section class="px-6">
          <div class="py-6">
            <h6 class="text-base font-semibold">
              Sorting
            </h6>
          </div>
          <div class="flex items-center">
            <div class="w-2/3">
              <el-select v-model="form.sorting.prop" class="w-full sort-property-select">
                <el-option
                  v-for="(label, key) in props.config.sorting"
                  :key="key"
                  :value="key"
                  :label="label"
                />
              </el-select>
            </div>
            <div class="w-1/3 -ml-px">
              <el-select v-model="form.sorting.order" class="w-full sort-order-select">
                <el-option value="ascending" label="Ascending" />
                <el-option value="descending" label="Descending" />
              </el-select>
            </div>
          </div>
        </section>
      </div>
    </template>
    <template #footer>
      <div style="flex: auto">
        <lf-button
          type="bordered"
          size="medium"
          class="mr-3"
          @click="isDrawerOpen = false"
        >
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          size="medium"
          :loading="sending"
          :disabled="$v.$invalid || (!!isEdit && !hasFormChanged)"
          @click="submit()"
        >
          {{ isEdit ? 'Update' : 'Add view' }}
        </lf-button>
      </div>
    </template>
  </app-drawer>
</template>

<script setup lang="ts">
import {
  computed, reactive, ref, watch,
} from 'vue';
import AppDrawer from '@/shared/drawer/drawer.vue';
import AppFormItem from '@/shared/form/form-item.vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import type { SavedView, SavedViewCreate, SavedViewsConfig } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import type { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import LfFilterItem from '@/shared/modules/filters/components/FilterItem.vue';
import { SavedViewsService } from '@/shared/modules/saved-views/services/saved-views.service';

import { ToastStore } from '@/shared/message/notification';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import formChangeDetector from '@/shared/form/form-change';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';

const props = defineProps<{
  modelValue: boolean,
  config: SavedViewsConfig,
  filters: Record<string, FilterConfig>,
  customFilters?: Record<string, FilterConfig>,
  placement: string,
  view: SavedView | SavedViewCreate | null,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): any,
  (e: 'reload'): any
}>();

const { hasPermission } = usePermissions();

const isEdit = computed(() => props.view && (props.view as SavedView).id);
const isDuplicate = computed(() => props.view && !(props.view as SavedView).id);

const title = computed<string>(() => {
  if (isEdit.value) {
    return 'Edit view';
  }
  if (isDuplicate.value) {
    return 'Duplicate view';
  }
  return 'New view';
});

const isDrawerOpen = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set(value: boolean) {
    emit('update:modelValue', value);
  },
});

const settingsDefaultValue = computed<Record<string, any>>(() => {
  const settingsObject = { ...props.config.settings };
  Object.entries(settingsObject).forEach(([key, setting]) => {
    settingsObject[key] = settingsObject[key].defaultValue;
  });
  return settingsObject;
});

interface SavedViewForm {
  shared: boolean;
  name: string;
  relation: 'and' | 'or';
  filters: Record<string, any>;
  settings: Record<string, any>;
  sorting: {
    prop: string;
    order: 'descending' | 'ascending';
  }
}

const form = reactive<SavedViewForm>({
  shared: false,
  name: '',
  relation: 'and',
  filters: {},
  // eslint-disable-next-line vue/no-ref-object-reactivity-loss -- intentional initial snapshot
  settings: { ...settingsDefaultValue.value },
  sorting: {
    prop: Object.keys(props.config.sorting).length > 0 ? Object.keys(props.config.sorting)[0] : props.config.defaultView.config.order.prop,
    order: 'descending',
  },
});

const rules = {
  name: {
    required,
  },
};

const { trackEvent } = useProductTracking();

const $v = useVuelidate(rules, form);
const { hasFormChanged, formSnapshot } = formChangeDetector(form);

const settings = computed<Record<string, any>>(() => {
  const settingsObject = { ...props.config.settings };
  Object.entries(settingsObject).forEach(([key, setting]) => {
    if (!setting.inSettings || !setting.settingsComponent) {
      delete settingsObject[key];
    }
  });
  return settingsObject;
});

// Filter dropdown
const dropdownSearch = ref<string>('');

const filteredFilters = computed(() => Object.entries(props.filters)
  .filter(([_, config]: [string, FilterConfig]) => config.label.toLowerCase().includes(dropdownSearch.value.toLowerCase())));

const allFilters = computed(() => ({
  ...props.filters,
  ...(props.customFilters || {}),
}));

// Filter list management
const filterList = ref<string[]>([]);
const openedFilter = ref<string>('');

const addFilter = (key: string) => {
  if (filterList.value.includes(key)) {
    return;
  }
  filterList.value.push(key);
  openedFilter.value = key;
  dropdownSearch.value = '';
};

const removeFilter = (key: any) => {
  openedFilter.value = '';
  filterList.value = filterList.value.filter((el) => el !== key);
  delete form.filters[key];
  dropdownSearch.value = '';
};

const fillForm = () => {
  if (props.view) {
    form.shared = hasPermission(LfPermission.customViewsTenantManage) ? props.view.visibility === 'tenant' : false;
    form.name = props.view.name;
    const {
      relation, order, settings, search, ...restFilters
    } = props.view.config;
    form.relation = relation;
    form.sorting.prop = order.prop;
    form.sorting.order = order.order;
    form.filters = { ...restFilters } || {};
    form.settings = { ...settings } || {};
    filterList.value = Object.keys(restFilters);
    formSnapshot();
  }
};

const reset = () => {
  form.shared = false;
  form.name = '';
  form.relation = 'and';
  form.filters = {};
  form.settings = { ...settingsDefaultValue.value };
  form.sorting = {
    prop: Object.keys(props.config.sorting).length > 0 ? Object.keys(props.config.sorting)[0] : props.config.defaultView.config.order.prop,
    order: 'descending',
  };
  filterList.value = [];
  $v.value.$reset();
  formSnapshot();
};

watch(() => props.view, () => {
  reset();
  if (props.view) {
    fillForm();
  }
}, { deep: true, immediate: true });

// Form submission
const sending = ref<boolean>(false);

const submit = (): void => {
  if ($v.value.$invalid) {
    return;
  }
  sending.value = true;
  const data: SavedViewCreate = {
    name: form.name,
    config: {
      search: '',
      relation: form.relation,
      order: {
        prop: form.sorting.prop,
        order: form.sorting.order,
      },
      settings: form.settings,
      ...form.filters,
    },
    placement: props.placement,
    visibility: form.shared ? 'tenant' : 'user',
  };
  if (isEdit.value) {
    trackEvent({
      key: FeatureEventKey.EDIT_CUSTOM_VIEW,
      type: EventType.FEATURE,
    });

    (form.shared ? ConfirmDialog({
      type: 'danger',
      title: 'Update shared view',
      message:
          'This view is shared with all workspace users, any changes will reflected in each user account.',
      icon: 'fa-arrows-rotate-reverse fa-light',
      cancelButtonText: 'Cancel',
      confirmButtonText: 'Update shared view',
    } as any) : Promise.resolve())
      .then(() => {
        SavedViewsService.update((props.view as SavedView).id, data)
          .then(() => {
            (window as any).analytics.track('Custom view updated', {
              placement: props.placement,
              name: form.name,
              visibility: form.shared ? 'tenant' : 'user',
              relation: form.relation,
              orderProperty: form.sorting.prop,
              orderDirection: form.sorting.order,
              settings: form.settings,
              filters: form.filters,
            });
            isDrawerOpen.value = false;
            reset();
            ToastStore.success('View updated successfully!');
            emit('reload');
          })
          .catch(() => {
            ToastStore.error('There was an error updating a view');
          })
          .finally(() => {
            sending.value = false;
          });
      });
  } else {
    if (isDuplicate.value) {
      trackEvent({
        key: FeatureEventKey.DUPLICATE_CUSTOM_VIEW,
        type: EventType.FEATURE,
      });
    } else {
      trackEvent({
        key: FeatureEventKey.ADD_CUSTOM_VIEW,
        type: EventType.FEATURE,
      });
    }

    SavedViewsService.create(data)
      .then(() => {
        if (isDuplicate.value) {
          ToastStore.success('View duplicated successfully!');
        } else {
          ToastStore.success('View successfully created!');
        }
        (window as any).analytics.track('Custom view created', {
          placement: props.placement,
          name: form.name,
          visibility: form.shared ? 'tenant' : 'user',
          relation: form.relation,
          orderProperty: form.sorting.prop,
          orderDirection: form.sorting.order,
          settings: form.settings,
          filters: form.filters,
          duplicated: isDuplicate.value,
        });
        isDrawerOpen.value = false;
        reset();
        emit('reload');
      })
      .catch(() => {
        if (isDuplicate.value) {
          ToastStore.error('There was an error duplicating a view');
        } else {
          ToastStore.error('There was an error creating a view');
        }
      })
      .finally(() => {
        sending.value = false;
      });
  }
};
</script>

<script lang="ts">
export default {
  name: 'LfSavedViewsForm',
};
</script>

<style lang="scss">
.sort-property-select{
  .el-input .el-input__wrapper, .el-input .el-textarea__inner{
    @apply rounded-r-none;
  }
}
.sort-order-select{
  .el-input .el-input__wrapper, .el-input .el-textarea__inner{
    @apply rounded-l-none;
  }
}
</style>
