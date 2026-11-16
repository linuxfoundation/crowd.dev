<template>
  <div class="grid gap-x-12 grid-cols-3">
    <h6>Company details</h6>
    <div class="col-span-2 organization-details-form">
      <el-form-item
        label="Name"
        prop="displayName"
        required
      >
        <el-input v-model="model.displayName" />
        <template #error>
          <div class="el-form-item__error">
            Name is required
          </div>
        </template>
      </el-form-item>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { Organization } from '../../types/Organization';

const props = defineProps<{
  modelValue: Organization;
}>();
const emit = defineEmits<{(e: 'update:modelValue', value: Organization): void }>();
const model = computed({
  get() {
    return props.modelValue;
  },
  set(newModel: Organization) {
    emit('update:modelValue', newModel);
  },
});
</script>
