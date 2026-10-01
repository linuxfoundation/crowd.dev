<template>
  <div>
    <lf-dropdown placement="bottom-end" width="14rem">
      <template #trigger>
        <lf-button type="secondary-ghost" icon-only>
          <lf-icon name="ellipsis" :size="24" class="text-gray-900" />
        </lf-button>
      </template>
      <lf-dropdown-item @click="editProject()">
        <lf-icon name="pen fa-sharp" />
        Edit project
      </lf-dropdown-item>
      <lf-dropdown-item type="danger" @click="deleteProject()">
        <lf-icon name="circle-minus" />
        Remove project
      </lf-dropdown-item>
    </lf-dropdown>
  </div>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';

const props = defineProps<{
  id: string,
}>();

const emit = defineEmits<{(e: 'onEditProject', id: string): void,
  (e: 'onDeleteProject', id: string): void,
}>();

const editProject = () => {
  emit('onEditProject', props.id);
};

const deleteProject = () => {
  emit('onDeleteProject', props.id);
};
</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectDropdown',
};
</script>
