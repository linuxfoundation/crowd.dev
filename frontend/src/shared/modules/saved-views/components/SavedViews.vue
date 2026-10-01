<template>
  <div class="flex items-end justify-between mb-6 h-8">
    <div v-if="scrollableTabs" class="border-b-2 border-r border-[#e4e7ed] flex-grow flex justify-end -mb-px pb-1">
      <el-dropdown placement="bottom-start">
        <lf-button
          type="primary-ghost"
          size="small"
          class="inset-y-0 !border-0 mr-2"
        >
          <lf-icon name="list-ul" :size="20" class="text-gray-400 flex items-center" />
        </lf-button>

        <template #dropdown>
          <el-dropdown-item
            :class="selectedTab === '' ? 'bg-primary-50' : ''"
            @click="selectedTab = ''"
          >
            {{ props.config.defaultView.name }}
          </el-dropdown-item>
          <el-dropdown-item
            v-for="view of views"
            :key="view.name"
            :class="selectedTab === view.id ? 'bg-primary-50' : ''"
            @click="selectedTab = view.id"
          >
            {{ view.name }}
          </el-dropdown-item>
        </template>
      </el-dropdown>
    </div>
    <div id="tabs" class="tabs flex-grow" :class="{ 'is-shrink': hasChanged }">
      <el-tabs v-model="selectedTab" @tab-change="onTabChange($event)">
        <el-tab-pane
          :label="props.config.defaultView.name"
          name=""
        />
        <el-tab-pane
          v-for="(view, vi) of (props.staticViews || [])"
          :key="view.name"
          :label="view.name"
          :name="view.id"
        >
          <template #label>
            {{ view.name }}
            <div
              v-if="vi >= (props.staticViews || []).length - 1"
              class="absolute right-0 top-0 h-full border-r-2 border-gray-200"
            />
          </template>
        </el-tab-pane>

        <el-tab-pane
          v-for="view of views"
          :key="view.name"
          :label="view.name"
          :name="view.id"
        />
      </el-tabs>
    </div>
    <div class="border-b-2 border-[#e4e7ed] flex-grow flex justify-end -mb-px pb-1">
      <lf-button
        v-if="hasChanged"
        type="primary-ghost"
        size="small"
        class="!leading-5 !h-8 mr-2"
        @click="reset()"
      >
        Reset view
      </lf-button>
      <el-dropdown v-if="hasChanged" placement="bottom-end">
        <lf-button
          type="primary-ghost"
          size="small"
          class="!leading-5 !h-8 mr-2"
        >
          Save as...
        </lf-button>
        <template #dropdown>
          <el-dropdown-item
            v-if="
              selectedTab.length > 0
                && selectedTab !== props.config.defaultView.id
                && selectedTab !== ''
                && (currentView.visibility !== 'tenant' || hasPermission(LfPermission.customViewsTenantManage))"
            @click="update()"
          >
            <div class="w-40 flex items-center">
              <lf-icon name="arrows-rotate-reverse" :size="16" class="text-gray-400 mr-2" />Update view
            </div>
          </el-dropdown-item>
          <el-dropdown-item v-if="hasPermission(LfPermission.customViewsCreate)" @click="createNewView()">
            <div class="w-40 flex items-center">
              <lf-icon name="plus" class="text-gray-400 mr-2" />Create new view
            </div>
          </el-dropdown-item>
        </template>
      </el-dropdown>

      <el-tooltip v-if="hasPermission(LfPermission.customViewsCreate)" content="Add view" placement="top">
        <lf-button
          type="primary-ghost"
          size="small"
          icon-only
          class="inset-y-0 !border-0 mr-2"
          @click="isFormOpen = true"
        >
          <lf-icon name="plus" :size="20" class="text-gray-400 flex items-center" />
        </lf-button>
      </el-tooltip>
      <el-popover trigger="click" placement="bottom-end" popper-class="!p-0" width="320px">
        <template #reference>
          <lf-button
            type="primary-ghost"
            size="small"
            icon-only
            class="inset-y-0 !border-0 mr-2"
            data-tooltip="Manage views"
          >
            <lf-icon name="gear" :size="20" class="text-gray-400 flex items-center" />
          </lf-button>
        </template>
        <lf-saved-views-management
          v-model:views="views"
          :config="props.config"
          @edit="edit($event)"
          @duplicate="duplicate($event)"
          @reload="getViews()"
        />
      </el-popover>
    </div>
  </div>
  <lf-saved-views-form
    v-model="isFormOpen"
    :config="props.config"
    :filters="props.filters"
    :custom-filters="props.customFilters"
    :placement="props.placement"
    :view="editView"
    @update:model-value="editView = null"
    @reload="getViews()"
  />
</template>

<script setup lang="ts">
import {
  computed,
  onMounted, onUnmounted, ref,
} from 'vue';
import type { Filter, FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { SavedView, SavedViewsConfig } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import { isEqual } from 'lodash';
import LfSavedViewsForm from '@/shared/modules/saved-views/components/forms/SavedViewForm.vue';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import LfSavedViewsManagement from '@/shared/modules/saved-views/components/SavedViewManagement.vue';
import { SavedViewsService } from '@/shared/modules/saved-views/services/saved-views.service';

import { ToastStore } from '@/shared/message/notification';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';

const props = defineProps<{
  modelValue: Filter,
  config: SavedViewsConfig,
  filters: Record<string, FilterConfig>,
  customFilters?: Record<string, FilterConfig>,
  placement: string,
  staticViews?: SavedView[],
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: Filter): any}>();

const { hasPermission } = usePermissions();

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

// Drawer
const isFormOpen = ref<boolean>(false);

const filters = computed<Filter>({
  get() {
    return props.modelValue;
  },
  set(value: Filter) {
    emit('update:modelValue', value);
  },
});

const selectedTab = ref<string>('');

const getView = (id: string): SavedView => {
  if (id.length > 0) {
    const view = allViews.value.find((v) => v.id === id);
    if (view) {
      return view;
    }
  }
  return props.config.defaultView;
};

const currentView = computed<SavedView>(() => getView(selectedTab.value));

const compareFilterToCurrentValues = (filter: Filter): boolean => {
  const compareFilter = {
    ...filter,
  };
  const currentFilter = {
    ...props.modelValue,
  };

  return isEqual(compareFilter, currentFilter);
};

// Check if scrollable tabs
const scrollableTabs = ref<boolean>(false);
const checkIfScrollableTabs = () => {
  setTimeout(() => {
    const tabs = document.getElementById('tabs');
    if (tabs) {
      const tabsContainer = tabs.querySelector('.el-tabs__nav-scroll');
      const tabsWrapper = tabs.querySelector('.el-tabs__nav');
      scrollableTabs.value = (tabsContainer?.clientWidth || 0) < (tabsWrapper?.clientWidth || 0);
    }
  }, 0);
};

const hasChanged = computed<boolean>(() => {
  checkIfScrollableTabs();
  const viewFilter = currentView.value.config;
  return !compareFilterToCurrentValues(viewFilter);
});

const onTabChange = (id: string) => {
  const { config } = getView(id);

  if (config) {
    filters.value = {
      ...config,
    };
  }
};

// Reset to current view
const reset = () => {
  filters.value = {
    ...currentView.value.config,
  };
};

// Change tab if filters match
const checkIfTabConfigMatch = () => {
  if (compareFilterToCurrentValues(props.config.defaultView.config)) {
    selectedTab.value = '';
    return;
  }
  const matchingView = views.value.find((view) => compareFilterToCurrentValues(view.config));
  if (matchingView) {
    selectedTab.value = matchingView.id;
  }
};

// View management
const views = ref<SavedView[]>([]);

const allViews = computed<SavedView[]>(() => [
  ...(props.staticViews || []),
  ...views.value,
]);

const getViews = () => {
  SavedViewsService.query({
    placement: [props.placement],
    segments: selectedProjectGroup.value?.id ? [selectedProjectGroup.value.id] : [],
  })
    .then((res: SavedView[]) => {
      views.value = [];
      setTimeout(() => {
        views.value = [...res];
        checkIfScrollableTabs();
        checkIfTabConfigMatch();
      }, 0);
    })
    .catch(() => {
      views.value = [];
    });
};

const editView = ref<Partial<SavedView> | null>(null);
const edit = (view: SavedView) => {
  editView.value = view;
  isFormOpen.value = true;
};
const duplicate = (view: SavedView) => {
  editView.value = {
    ...view,
    id: undefined,
    name: `${view.name} (1)`,
  };
  isFormOpen.value = true;
};

// Update current view
const update = () => {
  const view = currentView.value;
  (currentView.value.visibility === 'tenant' ? ConfirmDialog({
    type: 'danger',
    title: 'Update shared view',
    message:
        'This view is shared with all workspace users, any changes will reflected in each user account.',
    icon: 'fa-arrows-rotate-reverse fa-light',
    confirmButtonText: 'Update shared view',
    showCancelButton: true,
    cancelButtonText: 'Cancel',
  } as any) : Promise.resolve())
    .then(() => {
      SavedViewsService.update(view.id, {
        name: view.name,
        placement: view.placement,
        visibility: view.visibility,
        order: view.order,
        config: props.modelValue,
      })
        .then(() => {
          ToastStore.success('View updated successfully!');
          getViews();
        });
    });
};

const createNewView = () => {
  editView.value = {
    config: props.modelValue,
  };
  isFormOpen.value = true;
};

onMounted(() => {
  getViews();
  window.addEventListener('resize', checkIfScrollableTabs);
});

onUnmounted(() => {
  window.removeEventListener('resize', checkIfScrollableTabs);
});
</script>

<script lang="ts">
export default {
  name: 'LfSavedViews',
};
</script>

<style lang="scss" scoped>
.tabs {
  width: calc(100% - 113px);

  &.is-shrink{
    width: calc(100% - 303px);
  }
}
</style>
