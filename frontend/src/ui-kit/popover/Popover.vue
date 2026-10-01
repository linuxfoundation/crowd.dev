<template>
  <div class="c-popover">
    <div
      ref="trigger"
      class="c-popover__trigger"
      @click.stop="props.triggerEvent === 'click' ? togglePopover() : null"
      @mouseenter="props.triggerEvent === 'hover' ? showPopover() : null"
      @mouseleave="props.triggerEvent === 'hover' ? hidePopover() : null"
    >
      <slot name="trigger" :close="hidePopover" />
    </div>

    <Teleport v-if="!props.disabled && (props.persistent || isVisible)" to="body">
      <div
        ref="popover"
        :style="popoverStyle"
        class="c-popover__content"
        :class="[`is-placed-${props.placement}`, { 'is-hidden': props.persistent && !isVisible }, { 'popover-box': !props.isDropdown }]"
      >
        <slot :close="hidePopover" />
      </div>
    </Teleport>
  </div>
</template>

<script lang="ts" setup>
import {
  computed, onBeforeUnmount, ref, watch,
} from 'vue';
import type { PopoverPlacement } from '@/ui-kit/popover/types/PopoverPlacement';
import type { PopoverTrigger } from '@/ui-kit/popover/types/PopoverTrigger';

const props = withDefaults(defineProps<{
  placement?: PopoverPlacement,
  disabled?: boolean,
  spacing?: number,
  visibility?: boolean,
  triggerEvent?: PopoverTrigger,
  persistent?: boolean;
  isDropdown?: boolean;
  zIndex?: number | string;
}>(), {
  placement: 'bottom-start',
  disabled: false,
  spacing: 4,
  visibility: false,
  triggerEvent: 'click',
  persistent: false,
  isDropdown: false,
  zIndex: undefined,
});

const emit = defineEmits<{(e: 'update:visibility', value: boolean): void }>();

const trigger = ref<any>(null);
const popover = ref<any>(null);
const isVisible = ref(props.visibility);

watch(() => props.visibility, (newValue) => {
  isVisible.value = newValue;
}, { immediate: true });

watch(isVisible, (newValue) => {
  emit('update:visibility', newValue);
});

const togglePopover = () => {
  isVisible.value = !isVisible.value;
  if (isVisible.value) addOutsideClickListener();
  else removeOutsideClickListener();
};

const showPopover = () => {
  isVisible.value = true;
  addOutsideClickListener();
};

const hidePopover = () => {
  isVisible.value = false;
  removeOutsideClickListener();
};

const handleClickOutside = (event: any) => {
  if (
    !trigger.value?.contains(event.target)
      && !popover.value?.contains(event.target)
  ) {
    isVisible.value = false;
    removeOutsideClickListener();
  }
};

const addOutsideClickListener = () => {
  document.addEventListener('click', handleClickOutside);
};

const removeOutsideClickListener = () => {
  document.removeEventListener('click', handleClickOutside);
};

const popoverStyle = computed(() => {
  if (!trigger.value || !popover.value) return {};

  const triggerRect = trigger.value.getBoundingClientRect();
  return {
    '--lf-popover-trigger-top': `${triggerRect.top / 16}rem`,
    '--lf-popover-trigger-left': `${triggerRect.left / 16}rem`,
    '--lf-popover-trigger-right': `${triggerRect.right / 16}rem`,
    '--lf-popover-trigger-bottom': `${triggerRect.bottom / 16}rem`,
    '--lf-popover-trigger-width': `${trigger.value.offsetWidth / 16}rem`,
    '--lf-popover-trigger-height': `${trigger.value.offsetHeight / 16}rem`,
    '--lf-popover-content-width': `${popover.value.offsetWidth / 16}rem`,
    '--lf-popover-content-height': `${popover.value.offsetHeight / 16}rem`,
    '--lf-popover-spacing': `${props.spacing / 16}rem`,
    'z-index': props.zIndex,
  };
});

onBeforeUnmount(() => {
  removeOutsideClickListener();
});
</script>

<script lang="ts">
export default {
  name: 'LfPopover',
};
</script>
