<template>
  <lf-dropdown-item @click="emit('open-setting')">
    <lf-icon name="sliders-simple" type="regular" />
    Settings
  </lf-dropdown-item>
</template>

<script setup lang="ts">
import { defineProps } from 'vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineProps<{
  integration: any;
  segmentId: string;
  grandparentId: string;
}>();

const emit = defineEmits<{(e: 'open-setting'): void }>();
</script>

<script lang="ts">
export default {
  name: 'LfSchedDropdown',
};
</script>
