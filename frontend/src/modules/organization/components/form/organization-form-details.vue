<template>
  <div class="grid gap-x-12 grid-cols-3">
    <h6>Organization details</h6>
    <div class="col-span-2 organization-details-form">
      <el-form-item
        :label="fields.displayName.label"
        :prop="fields.displayName.name"
        required
      >
        <el-input v-model="model[fields.displayName.name]" />
        <template #error>
          <div class="el-form-item__error">
            Name is required
          </div>
        </template>
      </el-form-item>
    </div>
  </div>
</template>

<script setup>
import { defineEmits, defineProps, computed } from 'vue';

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => {},
  },
  fields: {
    type: Object,
    default: () => {},
  },
});
const emit = defineEmits(['update:modelValue']);
const model = computed({
  get() {
    return props.modelValue;
  },
  set(newModel) {
    emit('update:modelValue', newModel);
  },
});
</script>
