<template>
  <div>
    <lf-dropdown placement="bottom-end" :z-index="2300" width="14rem">
      <template #trigger>
        <lf-button type="secondary-ghost" icon-only>
          <lf-icon name="ellipsis" :size="24" class="text-gray-900" />
        </lf-button>
      </template>
      <lf-dropdown-item @click="editProject()">
        <lf-icon name="edit" />
        Edit project
      </lf-dropdown-item>
      <lf-dropdown-item @click="featuredProject()">
        <lf-icon :name="props.starred ? 'bookmark-slash' : 'bookmark'" />
        {{ props.starred ? 'Unmark' : 'Mark' }} as featured
      </lf-dropdown-item>
      <lf-dropdown-separator />
      <lf-dropdown-item @click="removeProject()">
        <lf-icon name="circle-minus" />
        Remove from collection
      </lf-dropdown-item>
    </lf-dropdown>
  </div>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';

const props = defineProps<{
  id: string;
  starred: boolean;
}>();

const emit = defineEmits<{(e: 'onEditProject', id: string): void;
  (e: 'onFeaturedProject', id: string): void;
  (e: 'onRemoveProject', id: string): void;
}>();

const editProject = () => {
  emit('onEditProject', props.id);
};

const featuredProject = () => {
  emit('onFeaturedProject', props.id);
};

const removeProject = () => {
  emit('onRemoveProject', props.id);
};
</script>

<script lang="ts">
export default {
  name: 'LfCollectionAddDropdown',
};
</script>
