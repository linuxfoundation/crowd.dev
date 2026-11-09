<template>
  <p class="text-medium">
    {{ props.data ? 'Yes' : 'No' }}
  </p>
</template>

<script setup lang="ts">
defineOptions({
  name: 'LfContributorAttributeBoolean',
});

const props = defineProps<{
  data: boolean,
}>();
</script>
