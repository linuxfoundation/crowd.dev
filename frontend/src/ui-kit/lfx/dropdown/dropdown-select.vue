<!--
Copyright (c) 2025 The Linux Foundation and each contributor.
SPDX-License-Identifier: MIT
-->
<template>
  <lfx-dropdown
    v-model:visibility="isVisible"
    :placement="props.placement"
    :width="props.width"
    :match-width="props.matchWidth"
    v-bind="$attrs"
  >
    <template #trigger>
      <slot
        name="trigger"
        :selected-option="selectedOption"
      />
    </template>

    <slot :selected-option="selectedOption" />
  </lfx-dropdown>
</template>

<script setup lang="ts">
import type { Placement } from '@popperjs/core';
import {
  computed, type VNode, provide, ref, useSlots,
} from 'vue';
import LfxDropdown from '@/ui-kit/lfx/dropdown/dropdown.vue';

const props = withDefaults(
  defineProps<{
    modelValue: string;
    placement?: Placement;
    width?: string;
    visibility?: boolean;
    matchWidth?: boolean;
  }>(),
  {
    placement: 'bottom-end',
    width: 'auto',
    visibility: false,
    matchWidth: false,
  },
);

const emit = defineEmits<{(e: 'update:modelValue', value: string): void;
  (e: 'update:visibility', value: boolean): void;
}>();

const slots = useSlots();

const value = computed({
  get: () => props.modelValue,
  set: (value: string) => emit('update:modelValue', value),
});

const isVisible = computed({
  get: () => props.visibility,
  set: (value: boolean) => emit('update:visibility', value),
});

// Recursive function to find components named 'LfxDropdownItem'
const findDropdownItems = (nodes: VNode[], result: VNode[] = []) => {
  nodes.forEach((node: VNode) => {
    if (!node) return;
    if (node.type && ['LfxDropdownItem', 'LfxOption'].includes((node.type as { name?: string }).name ?? '')) {
      result.push(node);
    }
    if (node.children && Array.isArray(node.children)) {
      findDropdownItems(node.children as VNode[], result);
    } else if (typeof node.children === 'object' && node.children?.default) {
      findDropdownItems((node.children?.default as () => VNode[])() as VNode[], result);
    }
  });
  return result;
};

const dropdownItems = computed<VNode[]>(() => {
  const slotNodes = slots.default ? slots.default() : [];
  return findDropdownItems(slotNodes as VNode[]);
});

const selectedOptionProps = ref<Record<string, string>>({});

const selectedOption = computed(() => {
  if (value.value === selectedOptionProps.value.value) {
    return selectedOptionProps.value;
  }
  const selected = dropdownItems.value.find((item) => item.props?.value === value.value);
  return selected?.props || { value: value.value, label: value.value };
});

provide('selectedValue', value);
provide('selectedOptionProps', selectedOptionProps);
</script>

<script lang="ts">
export default {
  name: 'LfxDropdownSelect',
};
</script>
