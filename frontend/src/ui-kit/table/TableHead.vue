<template>
  <th :class="props.sticky ? 'is-sticky' : ''">
    <div
      class="flex items-center gap-1"
      :class="[props.property ? 'cursor-pointer select-none' : '']"
      @click="sort()"
    >
      <slot />
      <lf-icon v-if="model === `${props.property}_DESC`" name="chevron-down" type="solid" :size="16" class="text-gray-500" />
      <lf-icon v-else-if="model === `${props.property}_ASC`" name="chevron-up" type="solid" :size="16" class="text-gray-500" />
      <div v-else-if="props.property">
        <lf-icon name="chevron-up" :size="10" class="text-gray-500" />
        <lf-icon name="chevron-down" :size="10" class="text-gray-500" />
      </div>
    </div>
  </th>
</template>

<script lang="ts" setup>
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'LfTableHead',
});

const props = defineProps<{
  modelValue?: string,
  property?: string,
  sticky?: boolean,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const model = computed<string>({
  get() {
    return props.modelValue || '';
  },
  set(value: string) {
    emit('update:modelValue', value);
  },
});

const sort = () => {
  if (model.value === `${props.property}_DESC`) {
    model.value = `${props.property}_ASC`;
  } else {
    model.value = `${props.property}_DESC`;
  }
};
</script>
