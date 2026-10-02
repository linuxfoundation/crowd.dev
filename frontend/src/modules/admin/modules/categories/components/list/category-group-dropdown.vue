<template>
  <lf-dropdown placement="bottom-end" width="14.5rem">
    <template #trigger>
      <slot />
    </template>

    <lf-dropdown-item @click="emit('edit')">
      <lf-icon name="edit" />
      Edit category group
    </lf-dropdown-item>

    <lf-dropdown-item type="danger" @click="isDeleteWarningOpen = true">
      <lf-icon name="trash-can" />
      Delete category group
    </lf-dropdown-item>
  </lf-dropdown>

  <!-- Delete warning modal -->
  <lf-modal
    v-model="isDeleteWarningOpen"
  >
    <div
      class="px-6 pt-6 pb-8 flex gap-4"
    >
      <div class="bg-red-50 h-10 w-10 min-w-10 rounded-full flex items-center justify-center">
        <lf-icon
          name="trash-can"
          class="text-red-500"
          :size="16"
          type="regular"
        />
      </div>
      <div class="pt-2 flex-grow">
        <h6 class="font-primary font-semibold pb-2">
          Are you sure you want to delete this category group?
        </h6>
        <p class="text-gray-500 text-medium">
          This will permanently delete the category group and all its associated categories. You can’t undo this action.
        </p>
      </div>
    </div>
    <div class="bg-gray-50 px-6 py-4.5 flex justify-end gap-4">
      <lf-button type="secondary-ghost" @click="isDeleteWarningOpen = false">
        Cancel
      </lf-button>
      <lf-button type="danger" :loading="deleting" @click="deleteCategoryGroup()">
        Delete category group
      </lf-button>
    </div>
  </lf-modal>
</template>

<script lang="ts" setup>
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { ref } from 'vue';
import { CategoryGroupService } from '@/modules/admin/modules/categories/services/category-group.service';
import type { CategoryGroup } from '@/modules/admin/modules/categories/types/CategoryGroup';
import { ToastStore } from '@/shared/message/notification';

const props = defineProps<{
  categoryGroup: CategoryGroup,
}>();

const emit = defineEmits<{(e: 'edit'): void;
  (e: 'reload'): void;
}>();

const isDeleteWarningOpen = ref(false);
const deleting = ref(false);

const deleteCategoryGroup = () => {
  if (deleting.value) {
    return;
  }
  deleting.value = true;
  CategoryGroupService.delete(props.categoryGroup.id)
    .then(() => {
      emit('reload');
      isDeleteWarningOpen.value = false;
      ToastStore.success('Category group deleted successfully');
    })
    .catch(() => {
      ToastStore.error('Error deleting category group');
    })
    .finally(() => {
      deleting.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfCategoryGroupDropdown',
};
</script>
