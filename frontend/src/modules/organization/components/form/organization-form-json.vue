<template>
  <div v-if="!!filteredValue" class="flex flex-col w-full gap-2">
    <el-input
      v-for="[key] in Object.entries(filteredValue)"
      :key="key"
      v-model="filteredValue[key]"
      disabled
    >
      <template #prefix>
        <span class="font-medium">
          {{ key }}:
        </span>
      </template>
    </el-input>
  </div>
  <el-input v-else disabled />
</template>

<script setup lang="ts">
import { computed } from 'vue';

type JsonValue = Record<string, string | number>;

const props = withDefaults(defineProps<{
  modelValue?: JsonValue | null;
  type?: string | null;
  filterValue?:((value: JsonValue) => JsonValue) | null;
}>(), {
  modelValue: null,
  type: null,
  filterValue: null,
});

const filteredValue = computed(() => {
  if (props.filterValue && props.modelValue) {
    return props.filterValue(props.modelValue);
  }

  return props.modelValue;
});

</script>
