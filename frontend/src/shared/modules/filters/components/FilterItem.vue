<template>
  <div class="flex items-center shadow rounded-md">
    <!-- Settings -->
    <el-popover
      v-model:visible="isOpen"
      teleported
      placement="bottom-start"
      width="420"
      trigger="click"
      :popper-class="`!p-0 filter-list-item-popper filter-${props.config.type} filter-${props.config.id}`"
    >
      <template #reference>
        <div
          class="border border-gray-100 rounded-l-md h-8 flex items-center py-1 px-2 bg-white cursor-pointer hover:bg-gray-100 transition"
          :class="[{ '!bg-gray-100': isOpen }, props.chipClasses]"
          data-qa="filter-list-chip"
        >
          <i class="text-base text-black mr-2" :class="config.iconClass" />
          <span
            class="text-xs text-gray-600 filter-item-text leading-6"
            v-html="
              $sanitize(
                (props.modelValue && config.itemLabelRenderer(props.modelValue, props.config.options, data))
                  || `<span class='!text-gray-500'>${config.label}...</span>`,
              )
            "
          />
        </div>
      </template>

      <div :data-qa-filter-type="props.config.type">
        <component
          :is="getComponent"
          v-if="getComponent"
          v-model="form"
          v-model:data="data"
          :config="props.config"
          v-bind="props.config.options"
        />
      </div>
      <div class="flex justify-end items-center border-t py-3 px-4">
        <lf-button type="primary-link" size="small" class="!h-8 mr-2" data-qa="filter-close" @click="close">
          Cancel
        </lf-button>
        <lf-button type="primary" size="small" :disabled="$v.$invalid" data-qa="filter-apply" @click="apply">
          Apply
        </lf-button>
      </div>
    </el-popover>

    <!-- Cancel -->
    <div
      v-if="!props.hideRemove"
      class="border border-gray-100 rounded-r-md h-8 flex items-center p-2 bg-white -ml-px hover:bg-gray-100 transition cursor-pointer group"
      data-qa="filter-list-chip-close"
      @click="emit('remove')"
    >
      <lf-icon name="xmark" :size="16" class="text-gray-500 group-hover:text-gray-900 flex items-center" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import type { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { filterComponentByType } from '@/shared/modules/filters/config/filterComponentByType';
import useVuelidate from '@vuelidate/core';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps<{
  modelValue: string,
  open: string,
  config: FilterConfig,
  hideRemove?: boolean,
  chipClasses?: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: any): void;
  (e: 'remove'): void;
  (e: 'update:open', value: string): void;
}>();

const form = ref({});
const data = ref({});

const isOpen = computed({
  get() {
    return props.open === props.config.id;
  },
  set(value) {
    if (value) {
      emit('update:open', props.config.id);
    } else if (props.config.id === props.open) {
      emit('update:open', '');
    }
  },
});

const getComponent = computed(() => {
  const { type, component } = props.config;
  if (type === FilterConfigType.CUSTOM) {
    return component;
  }
  return filterComponentByType[props.config.type];
});

const apply = () => {
  emit('update:modelValue', { ...form.value });
  close();
};

const close = () => {
  isOpen.value = false;
};

const $v = useVuelidate();

watch(
  () => props.modelValue,
  (value) => {
    form.value = { ...value };
  },
  { immediate: true, deep: true },
);
</script>

<script lang="ts">
export default {
  name: 'LfFilterItem',
};
</script>

<style lang="scss">
.filter-item-text {
  b {
    @apply font-medium text-gray-900 mr-2;
  }
}
</style>
