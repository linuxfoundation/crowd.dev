<template>
  <el-dropdown
    v-if="organization && hasPermissions"
    ref="dropdown"
    trigger="click"
    placement="bottom-end"
  >
    <slot name="trigger">
      <button
        class="el-dropdown-link btn p-1.5 rounder-md hover:bg-gray-200 text-gray-600"
        type="button"
        @click.prevent.stop
      >
        <lf-icon name="ellipsis" type="regular" :size="24" />
      </button>
    </slot>
    <template #dropdown>
      <app-organization-dropdown-content
        :organization="organization"
        :hide-edit="hideEdit"
        :hide-merge="hideMerge"
        @merge="emit('merge')"
        @close-dropdown="onDropdownClose"
        @unmerge="emit('unmerge')"
      />
    </template>
  </el-dropdown>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import type { DropdownInstance } from 'element-plus';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Organization } from '@/modules/organization/types/Organization';
import AppOrganizationDropdownContent from './organization-dropdown-content.vue';

defineOptions({ name: 'AppOrganizationDropdown' });

withDefaults(defineProps<{
  organization?: Organization;
  hideEdit?: boolean;
  hideMerge?: boolean;
}>(), {
  organization: undefined,
  hideEdit: false,
  hideMerge: false,
});

const emit = defineEmits<{(e: 'merge'): void, (e: 'unmerge'): void, (e: 'closeDropdown'): void }>();

defineSlots<{
  trigger?:() => unknown;
}>();

const { hasPermission } = usePermissions();

const hasPermissions = computed(() => [LfPermission.organizationEdit,
  LfPermission.organizationDestroy,
  LfPermission.mergeOrganizations]
  .some((permission) => hasPermission(permission)));

const dropdown = ref<DropdownInstance>();

const onDropdownClose = () => {
  dropdown.value?.handleClose();
  emit('closeDropdown');
};
</script>
