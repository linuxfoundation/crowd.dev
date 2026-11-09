<template>
  <div class="c-field" v-bind="$attrs">
    <div class="flex justify-between c-field__header">
      <slot name="label" :label-text="props.labelText">
        <label v-if="props.labelText" :for="props.for" class="c-field__label leading-5">
          {{ props.labelText }} <span v-if="props.required" class="c-field__required">*</span>
          <span v-if="props.description" class="text-small text-gray-500 font-normal"><br>{{ props.description }}</span>
        </label>
      </slot>
      <slot name="action" />
    </div>
    <slot />
  </div>
</template>

<script setup lang="ts">
defineOptions({
  name: 'LfField',
});

const props = withDefaults(defineProps<{
  labelText?: string,
  description?: string,
  required?: boolean;
  for?: string;
}>(), {
  labelText: undefined,
  description: undefined,
  required: false,
  for: undefined,
});
</script>
