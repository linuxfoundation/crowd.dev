<!-- src/components/ToastContainer.vue -->
<template>
  <div :class="positionClass" class="fixed z-[2126] space-y-2">
    <transition-group name="toast" tag="div">
      <div
        v-for="toast in store.toasts"
        :key="toast.id"
        class="c-notification"
        :class="toast.type"
      >
        <component :is="iconComponent(toast.type)" class="text-xl" />
        <div class="c-notification__group">
          <span class="c-notification__title flex-1">
            <component :is="messageComponent(toast.title)" />
          </span>
          <div v-if="toast.message" class="c-notification__content">
            <component :is="messageComponent(toast.message)" />
          </div>
        </div>
        <button
          type="button"
          class="c-notification__closeBtn ml-2 text-white hover:text-gray-200 text-lg leading-none"
          @click="store.remove(toast.id)"
        >
          <i class="fa-xmark fa-regular" />
        </button>
      </div>
    </transition-group>
  </div>
</template>

<script setup lang="ts">
import type { VNode } from 'vue';
import {
  computed, h, isVNode,
} from 'vue';
import type { NotificationTypes } from './notification';
import { ToastStore as store } from './notification';

const props = withDefaults(
  defineProps<{
    position?: 'top-right' | 'bottom-left' | 'bottom-right' | 'center';
  }>(),
  {
    position: 'bottom-right',
  },
);

const positionClass = computed(() => {
  switch (props.position) {
    case 'top-right':
      return 'top-4 right-4';
    case 'bottom-left':
      return 'bottom-4 left-4';
    case 'bottom-right':
      return 'bottom-4 right-4';
    case 'center':
      return 'top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2';
    default:
      return 'top-4 right-4';
  }
});

function iconComponent(type: NotificationTypes) {
  switch (type) {
    case 'success':
      return h('i', {
        class: 'fa-circle-check fa-light text-green-500',
      });
    case 'error':
      return h('i', {
        class: 'fa-circle-exclamation fa-light text-red-500',
      });
    default:
      return h('i', {
        class: 'fa-circle-notch fa-light text-primary-600 animate-spin',
      });
  }
}

function messageComponent(message: string | VNode) {
  return isVNode(message)
    ? message
    : h('span', null, message);
}
</script>

<script lang="ts">
export default {
  name: 'ToastContainer',
};
</script>

<style scoped>
.toast-enter-active,
.toast-leave-active {
  transition: all 0.3s ease;
}
.toast-enter-from {
  opacity: 0;
  transform: translateY(-10px);
}
.toast-leave-to {
  opacity: 0;
  transform: translateY(-10px);
}
</style>
