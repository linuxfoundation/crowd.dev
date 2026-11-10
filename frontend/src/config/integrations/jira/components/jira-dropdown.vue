<template>
  <lf-dropdown-item @click="emit('open-setting')">
    <lf-icon name="sliders-simple" type="regular" />
    Settings
  </lf-dropdown-item>
</template>

<script setup lang="ts">
import type { Integration } from '@/modules/admin/modules/integration/types/Integration';
import type { JiraSettings } from '@/config/integrations/jira/types/JiraSettings';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'LfGitDropdown',
});

defineProps<{
  integration: Integration<JiraSettings> | undefined,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const emit = defineEmits<{(e: 'open-setting'): void;
}>();

</script>
