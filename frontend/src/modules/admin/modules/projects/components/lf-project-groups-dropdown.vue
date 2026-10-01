<template>
  <div>
    <lf-dropdown placement="bottom-end" width="14rem">
      <template #trigger>
        <lf-button type="secondary-ghost" icon-only>
          <lf-icon name="ellipsis" :size="24" />
        </lf-button>
      </template>
      <lf-dropdown-item
        v-if="hasPermission(LfPermission.projectGroupEdit) && hasAccessToSegmentId(id)"
        @click="editProjectGroup()"
      >
        <lf-icon name="pen" />
        Edit project group
      </lf-dropdown-item>
      <lf-dropdown-item
        v-if="hasPermission(LfPermission.projectCreate) && hasAccessToSegmentId(id) && !showEditOnly"
        @click="addProject()"
      >
        <lf-icon name="plus" />
        Add project
      </lf-dropdown-item>
      <template
        v-if="((hasPermission(LfPermission.projectGroupEdit) && hasAccessToSegmentId(id))
          || (hasPermission(LfPermission.projectCreate) && hasAccessToSegmentId(id))) && !showEditOnly"
      >
        <lf-dropdown-separator />
        <lf-dropdown-item
          v-if="hasPermission(LfPermission.projectCreate) && hasAccessToSegmentId(id) && !showEditOnly"
          @click="updateSelectedProjectGroup(id)"
        >
          <lf-icon name="arrow-up-right-from-square" />
          View projects
        </lf-dropdown-item>
      </template>
    </lf-dropdown>
  </div>
</template>

<script setup>
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';

defineProps({
  id: {
    type: String,
    default: null,
  },
  showEditOnly: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(['onEditProjectGroup', 'onAddProject']);

const lsSegmentsStore = useLfSegmentsStore();
const { updateSelectedProjectGroup } = lsSegmentsStore;

const { hasPermission, hasAccessToSegmentId } = usePermissions();

const editProjectGroup = () => {
  emit('onEditProjectGroup');
};

const addProject = () => {
  emit('onAddProject');
};
</script>

<script>
export default {
  name: 'AppLfProjectGroupsDropdown',
};
</script>
