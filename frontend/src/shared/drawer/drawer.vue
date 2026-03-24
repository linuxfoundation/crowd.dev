<template>
  <teleport to="body">
    <el-drawer
      ref="drawerRef"
      v-model="model"
      :class="`${customClass} ${
        hasBorder ? 'bordered' : ''
      } ${
        hasPadding ? '' : 'no-padding'
      } ${
        hasScroll ? 'has-scroll' : ''
      }`"
      :show-close="false"
      :destroy-on-close="true"
      :close-on-click-modal="closeOnClickModal"
      :size="size"
      :z-index="zIndex"
      :before-close="closeFunction"
      @close="onClose"
    >
      <template #header="{ close, titleId, titleClass }">
        <div class="flex flex-col gap-1">
          <div class="flex grow justify-between items-start">
            <slot name="header">
              <div class="h-fit">
                <div
                  v-if="preTitle"
                  class="text-gray-600 text-2xs"
                >
                  {{ preTitle }}
                </div>
                <div class="flex items-center">
                  <slot name="beforeTitle" />
                  <h5
                    :id="titleId"
                    class="text-black grow-0"
                    :style="{ textWrap: 'nowrap' }"
                    :class="titleClass"
                  >
                    {{ title }}
                  </h5>
                  <slot name="afterTitle" />
                </div>
              </div>
            </slot>

            <div class="flex gap-3 items-center">
              <slot name="header-label" />
              <div class="ml-3">
                <lf-button
                  type="outline"
                  icon-only
                  @click="close"
                >
                  <lf-icon name="xmark" :size="20" class="text-gray-900 group-hover:text-primary-500" />
                </lf-button>
              </div>
            </div>
          </div>
          <slot name="belowTitle" />
        </div>
      </template>

      <template #default>
        <slot name="content" />
      </template>
      <template v-if="showFooter" #footer>
        <slot name="footer" />
      </template>
    </el-drawer>
  </teleport>
</template>

<script setup>
import {
  computed, ref, watch, onUnmounted, nextTick,
} from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { number } from 'yup';

const emit = defineEmits(['update:modelValue', 'close']);
const props = defineProps({
  modelValue: {
    type: Boolean,
    default: () => false,
  },
  preTitle: {
    type: String,
    default: () => null,
  },
  title: {
    type: String,
    required: true,
  },
  customClass: {
    type: String,
    default: () => '',
  },
  direction: {
    type: String,
    default: () => 'rtl',
  },
  size: {
    type: [String, Number],
    default: () => '40%',
  },
  showFooter: {
    type: Boolean,
    default: () => true,
  },
  hasBorder: {
    type: Boolean,
    default: () => false,
  },
  hasPadding: {
    type: Boolean,
    default: () => true,
  },
  zIndex: {
    type: number,
    default: () => 2004,
  },
  closeOnClickModal: {
    type: Boolean,
    default: () => false,
  },
  closeFunction: {
    type: Function,
    default: (done) => {
      done(false);
    },
  },
});

const model = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    emit('update:modelValue', value);
  },
});

const drawerRef = ref(null);
const hasScroll = ref(false);
let resizeObserver = null;
let mutationObserver = null;
let drawerBodyEl = null;

const getDrawerBody = () => {
  const drawers = document.querySelectorAll('.el-drawer');
  if (drawers.length === 0) return null;
  const drawer = drawers[drawers.length - 1];
  return drawer.querySelector('.el-drawer__body');
};

const checkScroll = () => {
  if (!drawerBodyEl) return;
  hasScroll.value = drawerBodyEl.scrollHeight > drawerBodyEl.clientHeight;
};

const setupObservers = () => {
  drawerBodyEl = getDrawerBody();
  if (!drawerBodyEl) return;

  // Check initial state
  checkScroll();

  // Observe size changes
  resizeObserver = new ResizeObserver(checkScroll);
  resizeObserver.observe(drawerBodyEl);

  // Observe content changes (children added/removed)
  mutationObserver = new MutationObserver(checkScroll);
  mutationObserver.observe(drawerBodyEl, { childList: true, subtree: true });
};

const cleanupObservers = () => {
  if (resizeObserver) {
    resizeObserver.disconnect();
    resizeObserver = null;
  }
  if (mutationObserver) {
    mutationObserver.disconnect();
    mutationObserver = null;
  }
  drawerBodyEl = null;
  hasScroll.value = false;
};

watch(model, (visible) => {
  if (visible) {
    nextTick(() => {
      // Wait for drawer animation to complete
      setTimeout(setupObservers, 300);
    });
  } else {
    cleanupObservers();
  }
}, { immediate: true });

onUnmounted(() => {
  cleanupObservers();
});

const onClose = () => {
  model.value = false;
  emit('close');
};
</script>

<script>
export default {
  name: 'AppDrawer',
};
</script>
