<template>
  <div class="inline-select-input">
    <el-dropdown
      :placement="popperPlacement"
      :popper-class="popperClass"
      trigger="click"
      @visible-change="handleDropdownVisibleChange"
    >
      <div class="flex items-center" data-qa="filter-inline-select">
        <span class="inline-select-input-prefix mr-1">{{
          prefix
        }}</span>
        <span class="inline-select-input-value">{{
          modelLabel
        }}</span>
        <lf-icon
          name="chevron-down"
          :size="16"
          class="ml-1"
          :style="
            dropdownExpanded
              ? 'transform: rotate(180deg)'
              : ''
          "
        />
      </div>
      <template #dropdown>
        <el-dropdown-item
          v-for="option of computedOptions"
          :key="`option-${option.value}`"
          :class="{
            '!h-fit !py-2.5': option.description,
            'is-selected': option.selected,
          }"
          data-qa="filter-inline-select-option"
          @click="handleOptionClick(option)"
        >
          <div class="flex flex-col">
            <span>{{ option.label }}</span>
            <span
              v-if="option.description"
              class="text-2xs text-gray-500"
            >
              {{ option.description }}
            </span>
          </div>
        </el-dropdown-item>
      </template>
    </el-dropdown>
  </div>
</template>

<script setup lang="ts">
import {
  ref,
  computed,
} from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({ name: 'AppInlineSelectInput' });

const props = withDefaults(defineProps<{
  modelValue?: string | number | unknown[] | null;
  options?: { value: string | number; label: string; description?: string }[];
  prefix?: string | null;
  popperClass?: string | null;
  popperPlacement?: string;
}>(), {
  modelValue: null,
  options: () => [],
  prefix: null,
  popperClass: null,
  popperPlacement: 'top-start',
});

const emit = defineEmits<{(e: 'update:modelValue', value: string | number | unknown[] | null): void;
  (e: 'change', value: string | number | unknown[] | null): void;
}>();

const model = computed<string | number | unknown[] | null>({
  get() {
    return props.modelValue;
  },
  set(value) {
    emit('update:modelValue', value);
    emit('change', value);
  },
});

const computedOptions = computed(() => props.options.map((o) => ({
  ...o,
  selected: o.value === model.value,
})));

const modelLabel = computed(() => props.options.find((o) => o.value === model.value)
  ?.label);

const dropdownExpanded = ref(false);
const handleDropdownVisibleChange = (value: boolean): void => {
  dropdownExpanded.value = value;
};
const handleOptionClick = (option: { value: string | number }): void => {
  model.value = option.value;
};
</script>

<style lang="scss">
.inline-select-input {
  @apply leading-none;
  i {
    transition: transform 0.2s ease;
  }
  &-prefix {
    @apply text-gray-500;
  }
  &-value {
    @apply text-black;
  }
}
</style>
