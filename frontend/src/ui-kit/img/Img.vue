<template>
  <img v-show="showImage" :src="props.src" :alt="props.alt" @error="showImage = false" />
</template>

<script setup lang="ts">
import { ref, watch } from 'vue';

defineOptions({
  name: 'LfImg',
});

const props = defineProps<{
  src: string,
  alt: string,
}>();

const showImage = ref<boolean>(true);

watch(() => props.src, () => {
  showImage.value = true;
}, { immediate: true });
</script>
