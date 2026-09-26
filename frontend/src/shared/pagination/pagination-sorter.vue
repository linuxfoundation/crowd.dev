<template>
  <div
    class="flex grow gap-8 items-center pagination-sorter"
    :class="sorterClass"
  >
    <div class="flex items-center gap-0.5">
      <span
        v-if="total"
        id="totalCount"
        data-qa="members-total"
        class="text-gray-500 text-sm"
      ><span v-if="hasPageCounter">{{ count.minimum.toLocaleString('en') }}-{{
         count.maximum.toLocaleString('en')
       }}
         of
       </span>
        {{ computedLabel }}</span>

      <slot name="defaultFilters" />
    </div>
    <div class="flex items-center">
      <app-inline-select-input
        v-if="sorter"
        v-model="model"
        popper-class="sorter-popper-class"
        :placement="sorterPopperPlacement"
        prefix="Show:"
        :options="computedOptions"
        @change="onChange"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import pluralize from 'pluralize';

defineOptions({ name: 'AppPaginationSorter' });

const emit = defineEmits<{(e: 'changeSorter', value: string | number): void;
  (e: 'update:modelValue', value: string | number | null): void;
  (e: 'export'): void;
}>();
const props = withDefaults(defineProps<{
  currentPage: number;
  pageSize: number;
  total: number;
  position?: 'bottom' | 'top';
  hasPageCounter?: boolean;
  module?: string;
  modelValue?: string | null;
  sorter?: boolean;
  export?:() => unknown;
}>(), {
  position: 'bottom',
  hasPageCounter: true,
  module: '',
  modelValue: null,
  sorter: true,
  export: () => false,
});

defineSlots<{
  defaultFilters?:() => unknown;
}>();

const model = computed<string | number | null>({
  get() {
    if (
      props.module !== 'activity'
    ) {
      return props.pageSize;
    }

    return props.modelValue;
  },

  set(value: string | number | null) {
    emit('update:modelValue', value);
  },
});

const computedOptions = computed(() => {
  if (props.module === 'activity') {
    return [
      {
        value: 'trending',
        label: 'Trending',
      },
      {
        value: 'recentActivity',
        label: 'Most recent activity',
      },
    ];
  }

  return [
    { value: 20, label: '20' },
    { value: 50, label: '50' },
    { value: 100, label: '100' },
    { value: 200, label: '200' },
  ];
});

const computedLabel = computed(() => pluralize(props.module === 'member' ? 'person' : props.module, props.total, true));

const count = computed(() => ({
  minimum:
    props.currentPage * props.pageSize
    - (props.pageSize - 1),
  maximum: Math.min(
    props.currentPage * props.pageSize
      - (props.pageSize - 1)
      + props.pageSize
      - 1,
    props.total,
  ),
}));
// Dynamic class for sorter alignment in the page
const sorterClass = computed(() => {
  if (props.position === 'bottom') {
    return 'justify-end';
  }

  return 'justify-between';
});
// Dynamic placement for sorter popper in the page
const sorterPopperPlacement = computed(() => {
  if (props.position === 'bottom') {
    return 'top-end';
  }

  return 'bottom-end';
});

const onChange = (value: string | number) => {
  emit('changeSorter', value);
};
</script>
