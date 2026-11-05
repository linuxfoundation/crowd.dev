<template>
  <div v-if="isDrawerOpened">
    <teleport to="body">
      <div class="c-drawer" @click="clickOutsideClose()">
        <div class="c-drawer__content" :style="{ 'max-width': props.width }" v-bind="$attrs" @click.stop>
          <slot :close="close" />
        </div>
      </div>
    </teleport>
  </div>
</template>

<script lang="ts" setup>
import { computed, watch } from 'vue';

const props = withDefaults(defineProps<{
  modelValue: boolean,
  width?: string,
  closeFunction?:() => boolean,
}>(), {
  width: '37.5rem',
  closeFunction: () => true,
});

const emit = defineEmits<{(e:'update:modelValue', value: boolean): void}>();

const isDrawerOpened = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set(value: boolean) {
    emit('update:modelValue', value);
  },
});

const close = () => {
  emit('update:modelValue', false);
};

const clickOutsideClose = () => {
  const canClose = props.closeFunction();
  if (canClose) {
    close();
  }
};

const onEscapeKeyUp = (event: KeyboardEvent) => {
  if (event.which === 27) {
    clickOutsideClose();
  }
};

watch(() => isDrawerOpened.value, (show: boolean) => {
  if (!show) {
    window.removeEventListener('keyup', onEscapeKeyUp);
  } else {
    window.addEventListener('keyup', onEscapeKeyUp);
  }
}, { immediate: true });
</script>

<script lang="ts">
export default {
  name: 'LfDrawer',
};
</script>
