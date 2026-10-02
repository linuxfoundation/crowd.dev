<template>
  <div class="flex justify-start items-center border-b border-gray-100 p-4">
    <lf-switch
      v-model="model"
      class="w-fit flex !justify-start filter-include-switch"
      size="tiny"
      data-qa="filter-include-switch"
    >
      {{ modelValue ? 'Include' : 'Exclude' }}
    </lf-switch>

    <el-tooltip
      content="If excluded, turns the filter into a negative condition"
      placement="top"
    >
      <lf-icon name="circle-question" :size="16" class="text-gray-400" />
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';

const props = defineProps<{
  modelValue: boolean
}>();
const emit = defineEmits(['update:modelValue']);
const model = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set(v) {
    emit('update:modelValue', v);
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfFilterIncludeSwitch',
};
</script>

<style lang="scss">
.filter-include-switch{
   @apply grow-0;
}
</style>
