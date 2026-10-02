<template>
  <button
    :type="props.nativeType"
    class="c-btn"
    :class="[
      `c-btn--${props.type}`,
      `c-btn--${props.size}`,
      { 'is-loading': props.loading },
      { 'c-btn--icon': props.iconOnly },
    ]"
    :disabled="props.disabled"
  >
    <lf-icon v-if="props.loading" name="circle-notch" :size="14" class="c-btn__loader text-base" />
    <span v-if="props.loading && props.loadingText && !props.iconOnly">{{ props.loadingText }}</span>
    <slot v-else />
  </button>
</template>

<script setup lang="ts">
import type { ButtonType } from '@/ui-kit/button/types/ButtonType';
import type { ButtonSize } from '@/ui-kit/button/types/ButtonSize';
import type { ButtonNativeType } from '@/ui-kit/button/types/ButtonNativeType';

const props = withDefaults(defineProps<{
  type?: ButtonType,
  size?: ButtonSize,
  iconOnly?: boolean;
  nativeType?: ButtonNativeType;
  loading?: boolean;
  loadingText?: string;
  disabled?: boolean;
}>(), {
  type: 'primary',
  size: 'medium',
  iconOnly: false,
  nativeType: 'button',
  loading: false,
  loadingText: undefined,
  disabled: false,
});
</script>

<script lang="ts">
export default {
  name: 'LfButton',
};
</script>
