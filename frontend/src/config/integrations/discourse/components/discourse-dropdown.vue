<template>
  <lf-dropdown-item @click="emit('open-setting')">
    <lf-icon name="sliders-simple" type="regular" />
    Settings
  </lf-dropdown-item>
</template>

<script setup lang="ts">
import type { Integration } from '@/modules/admin/modules/integration/types/Integration';
import type { DiscourseSettings } from '@/config/integrations/discourse/types/DiscourseSettings';

import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'LfDiscourseDropdown',
});

defineProps<{
  integration: Integration<DiscourseSettings> | undefined,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const emit = defineEmits<{(e: 'open-setting'): void;
}>();
</script>
