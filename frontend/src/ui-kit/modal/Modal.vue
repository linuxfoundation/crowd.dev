<template>
  <div v-if="isModalOpened">
    <teleport to="#teleport-modal" :disabled="!props.teleport">
      <div class="c-modal" :class="containerClass" @click="clickOutsideClose()">
        <div
          class="c-modal__content"
          :class="contentClass"
          :style="{ 'max-width': props.width }"
          v-bind="$attrs"
          @click.stop
        >
          <div v-if="headerTitle" class="c-modal__header">
            <div class="flex flex-col">
              <div v-if="preTitle" class="text-gray-600 text-2xs">
                {{ preTitle }}
              </div>
              <h5>{{ headerTitle }}</h5>
            </div>
            <lf-button type="secondary-ghost" icon-only @click="close()">
              <lf-icon name="xmark" type="regular" />
            </lf-button>
          </div>
          <slot :close="close" />
        </div>
      </div>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { computed, watch } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = withDefaults(
  defineProps<{
    modelValue: boolean;
    width?: string;
    containerClass?: string;
    contentClass?: string;
    headerTitle?: string;
    preTitle?: string;
    teleport?: boolean;
    closeFunction?:() => boolean;
  }>(),
  {
    width: '37.5rem',
    closeFunction: () => true,
    containerClass: '',
    contentClass: '',
    headerTitle: '',
    preTitle: '',
    teleport: true,
  },
);

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void}>();

const isModalOpened = computed<boolean>({
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

watch(
  () => isModalOpened.value,
  (show: boolean) => {
    if (!show) {
      window.removeEventListener('keyup', onEscapeKeyUp);
    } else {
      window.addEventListener('keyup', onEscapeKeyUp);
    }
  },
);
</script>

<script lang="ts">
export default {
  name: 'LfModal',
};
</script>
