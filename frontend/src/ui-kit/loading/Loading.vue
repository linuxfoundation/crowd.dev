<template>
  <div>
    <div
      v-for="el of new Array(props.count)"
      :key="el"
      class="c-loading"
      v-bind="$attrs"
      :style="{ height: props.height, width: props.width }"
    />
  </div>
</template>

<script setup lang="ts">
defineOptions({
  name: 'LfLoading',
});

const props = withDefaults(defineProps<{
  height?: string,
  width?: string,
  count?: number,
}>(), {
  height: '100px',
  width: '100%',
  count: 1,
});
</script>
