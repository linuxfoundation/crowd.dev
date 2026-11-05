<template>
  <lf-dropdown-item @click="emit('open-setting')">
    <lf-icon name="sliders-simple" type="regular" />
    Settings
  </lf-dropdown-item>
</template>

<script setup lang="ts">
import type { Integration } from '@/modules/admin/modules/integration/types/Integration';
import type { GitHubSettings } from '@/config/integrations/github-nango/types/GithubSettings';
import { defineProps } from 'vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineProps<{
  integration: Integration<GitHubSettings> | undefined;
  segmentId: string | null;
  grandparentId: string | null;
}>();

const emit = defineEmits<{(e: 'open-setting'): void;
}>();
</script>

<script lang="ts">
export default {
  name: 'LfGitHubDropdown',
};
</script>
