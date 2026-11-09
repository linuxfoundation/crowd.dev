<template>
  <div
    class="text-medium"
    v-html="props.data.substring(0, showMore ? props.data.length : 150) || '-'"
  />
  <lf-button
    v-if="props.data.length > 150"
    type="primary-link"
    size="small"
    class="mt-3"
    @click="showMore = !showMore"
  >
    Show {{ showMore ? 'less' : 'more' }}
  </lf-button>
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import { ref } from 'vue';

defineOptions({
  name: 'LfContributorAttributeString',
});

const props = defineProps<{
  data: string,
}>();

const showMore = ref<boolean>(false);
</script>
