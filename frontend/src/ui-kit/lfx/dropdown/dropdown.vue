<!--
Note: This is a copy of the LFX Insights implementation of the Dropdown component.
This will replace the old Popover component in the future.
-->
<template>
  <lfx-popover
    ref="popover"
    v-model:visibility="isVisible"
    :placement="props.placement"
    v-bind="$attrs"
    :is-modal="pageWidth < 640"
    :match-width="props.matchWidth"
    :popover-class="props.popoverClass"
  >
    <slot name="trigger" />

    <template #content>
      <div
        class="lfx-c-dropdown overflow-y-auto"
        :style="{ width: props.width }"
        :class="props.dropdownClass"
        @click="popover?.closePopover()"
      >
        <slot />
      </div>
    </template>
  </lfx-popover>
</template>

<script setup lang="ts">
import { ref, computed, provide } from 'vue';
import type { Placement } from '@popperjs/core';
import LfxPopover from '@/ui-kit/lfx/popover/popover.vue';
import useResponsive from '@/utils/responsive';

defineOptions({
  name: 'LfxDropdown',
});

const props = withDefaults(
  defineProps<{
    placement?: Placement;
    width?: string;
    visibility?: boolean;
    matchWidth?: boolean;
    dropdownClass?: string;
    popoverClass?: string;
  }>(),
  {
    placement: 'bottom-end',
    width: 'auto',
    visibility: false,
    matchWidth: false,
    dropdownClass: '',
    popoverClass: '',
  },
);

const emit = defineEmits<{(e: 'update:visibility', value: boolean): void }>();

const submenuOpen = ref('');

provide('submenuOpen', submenuOpen);

const isVisible = computed({
  get: () => props.visibility,
  set: (value: boolean) => {
    emit('update:visibility', value);
    submenuOpen.value = '';
  },
});

const popover = ref<typeof LfxPopover | null>(null);

const { pageWidth } = useResponsive();
</script>
