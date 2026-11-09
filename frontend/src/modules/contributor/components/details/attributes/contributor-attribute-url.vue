<template>
  <a
    :href="props.data"
    target="_blank"
    rel="noopener noreferrer"
    class="text-medium !text-black underline decoration-dashed
             decoration-gray-400 underline-offset-4 hover:decoration-gray-900"
  >
    {{ props.data }}
  </a>
</template>

<script setup lang="ts">
defineOptions({
  name: 'LfContributorAttributeUrl',
});

const props = defineProps<{
  data: string,
}>();
</script>
