<template>
  <div
    v-if="(hasPermission(LfPermission.subProjectCreate) && hasAccessToSegmentId(id ?? ''))
      || (hasPermission(LfPermission.projectEdit) && hasAccessToSegmentId(id ?? ''))"
  >
    <lf-dropdown placement="bottom-end" width="12rem">
      <template #trigger>
        <lf-button type="secondary-ghost-light" icon-only>
          <lf-icon name="ellipsis" type="regular" />
        </lf-button>
      </template>
      <lf-dropdown-item
        v-if="(hasPermission(LfPermission.projectEdit) && hasAccessToSegmentId(id ?? ''))"
        @click="editProject()"
      >
        <lf-icon name="pen" />
        Edit project
      </lf-dropdown-item>
      <lf-dropdown-item
        v-if="(hasPermission(LfPermission.subProjectCreate) && hasAccessToSegmentId(id ?? ''))"
        @click="addSubProject()"
      >
        <lf-icon name="plus" />
        Add sub-project
      </lf-dropdown-item>
    </lf-dropdown>
  </div>
</template>

<script setup lang="ts">
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';

defineOptions({ name: 'AppLfProjectsDropdown' });

withDefaults(defineProps<{
  id?: string | null;
}>(), {
  id: null,
});

const emit = defineEmits<{(e: 'onEditProject'): void;
  (e: 'onAddSubProject'): void;
}>();

const { hasPermission, hasAccessToSegmentId } = usePermissions();

const editProject = () => {
  emit('onEditProject');
};

const addSubProject = () => {
  emit('onAddSubProject');
};
</script>
