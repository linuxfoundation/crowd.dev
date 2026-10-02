<template>
  <div>
    <lf-dropdown placement="bottom-end" width="14rem">
      <template #trigger>
        <lf-button type="secondary-ghost" icon-only>
          <lf-icon name="ellipsis" :size="24" class="text-gray-900" />
        </lf-button>
      </template>
      <lf-dropdown-item @click="editCollection()">
        <lf-icon name="pen fa-sharp" />
        Edit collection
      </lf-dropdown-item>
      <lf-dropdown-item @click="starCollection()">
        <lf-icon name="star" />
        {{ collection.starred ? 'Unmark' : 'Mark' }} as featured
      </lf-dropdown-item>
      <lf-dropdown-item type="danger" @click="deleteCollection()">
        <lf-icon name="trash-can" />
        Delete collection
      </lf-dropdown-item>
    </lf-dropdown>
  </div>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import type { CollectionModel } from '../models/collection.model';

const props = defineProps<{
  id: string,
  collection: CollectionModel
}>();

const emit = defineEmits<{(e: 'onEditCollection', id: string): void,
  (e: 'onDeleteCollection', id: string): void,
  (e: 'onStarCollection', id: string): void,
}>();

const editCollection = () => {
  emit('onEditCollection', props.id);
};

const deleteCollection = () => {
  emit('onDeleteCollection', props.id);
};

const starCollection = () => {
  emit('onStarCollection', props.id);
};
</script>

<script lang="ts">
export default {
  name: 'LfCollectionDropdown',
};
</script>
