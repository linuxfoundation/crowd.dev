<template>
  <div class="px-2">
    <div class="py-2">
      <article class="py-2.5 px-3 flex justify-between">
        <p class="text-xs text-gray-900">
          {{ props.config.defaultView.name }}
        </p>
        <p class="text-xs text-gray-400 italic">
          default view
        </p>
      </article>
    </div>
    <div class="border-b border-gray-100" />
    <div class="py-2">
      <vue-draggable-next :list="views" @change="onListChange">
        <article
          v-for="view of views"
          :key="view.id"
          class="p-2 rounded flex items-center justify-between flex-grow transition hover:bg-gray-50 cursor-grab"
        >
          <div class="flex items-center">
            <lf-icon name="grip-dots-vertical" type="solid" :size="14" class="text-gray-400 mr-2" />
            <span class="text-sm leading-5 text-black">{{ view.name }}</span>
          </div>
          <div class="flex items-center">
            <el-tooltip placement="top" content="Edit view">
              <div
                v-if="view.visibility !== 'tenant' || hasPermission(LfPermission.customViewsTenantManage)"
                class="h-6 w-6 flex items-center justify-center ml-1 group cursor-pointer hover:bg-gray-100 rounded"
                @click="edit(view)"
              >
                <lf-icon name="pen fa-sharp" :size="14" class="text-gray-400 group-hover:text-gray-600" />
              </div>
            </el-tooltip>
            <el-tooltip placement="top" content="Duplicate view">
              <div
                class="h-6 w-6 flex items-center justify-center ml-1 group cursor-pointer hover:bg-gray-100 rounded"
                @click="duplicate(view)"
              >
                <lf-icon name="copy" :size="14" class="text-gray-400 group-hover:text-gray-600" />
              </div>
            </el-tooltip>
            <el-tooltip placement="top" content="Delete view">
              <div
                v-if="view.visibility !== 'tenant' || hasPermission(LfPermission.customViewsTenantManage)"
                class="h-6 w-6 flex items-center justify-center ml-1 group cursor-pointer hover:bg-gray-100 rounded"
                @click="remove(view)"
              >
                <lf-icon name="trash-can" :size="14" class="text-gray-400 group-hover:text-gray-600" />
              </div>
            </el-tooltip>
          </div>
        </article>
      </vue-draggable-next>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { SavedView, SavedViewsConfig } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import { VueDraggableNext } from 'vue-draggable-next';
import { computed } from 'vue';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import { SavedViewsService } from '@/shared/modules/saved-views/services/saved-views.service';

import { ToastStore } from '@/shared/message/notification';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  config: SavedViewsConfig,
  views: SavedView[]
}>();

const emit = defineEmits<{(e: 'update:views', value: SavedView[]): any,
  (e: 'edit', value: SavedView): any,
  (e: 'duplicate', value: SavedView): any,
  (e: 'reload'): any,
}>();

const { trackEvent } = useProductTracking();

const { hasPermission } = usePermissions();

const views = computed<SavedView[]>({
  get() {
    return props.views;
  },
  set(value: SavedView[]) {
    emit('update:views', value);
  },
});

const onListChange = () => {
  trackEvent({
    key: FeatureEventKey.REORDER_CUSTOM_VIEW,
    type: EventType.FEATURE,
  });

  SavedViewsService.updateBulk(
    views.value.map(
      (view, viewIndex) => ({
        id: view.id,
        name: view.name,
        order: viewIndex + 1,
      }),
    ),
  ).then(() => {
    (window as any).analytics.track('Custom views rearanged');
    emit('reload');
  });
};

const edit = (view: SavedView) => {
  emit('edit', view);
};
const duplicate = (view: SavedView) => {
  emit('duplicate', view);
};
const remove = (view: SavedView) => {
  const isShared = view.visibility === 'tenant';
  ConfirmDialog({
    type: 'danger',
    title: isShared ? 'Delete shared view' : 'Delete view',
    message: isShared
      ? 'This view will be deleted on all user accounts from this workspace. Are you sure you want to proceed? You can’t undo this action.'
      : 'Are you sure you want to proceed? You can’t undo this action.',
    icon: 'fa-trash-can fa-light',
    cancelButtonText: 'Cancel',
    confirmButtonText: isShared ? 'Delete shared view' : 'Delete view',
  } as any)
    .then(() => {
      trackEvent({
        key: FeatureEventKey.DELETE_CUSTOM_VIEW,
        type: EventType.FEATURE,
      });

      SavedViewsService.delete(view.id)
        .then(() => {
          (window as any).analytics.track('Custom view deleted', {
            placement: view.placement,
            name: view.name,
          });
          emit('reload');
          ToastStore.success('View successfully deleted');
        })
        .catch(() => {
          ToastStore.error('There was an error deleting view');
        });
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfSavedViewsManagement',
};
</script>

<style lang="scss">

</style>
