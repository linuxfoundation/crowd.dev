<template>
  <p class="text-medium">
    {{ props.data.substring(0, showMore ? props.data.length : limit) || '-' }}
  </p>
  <lf-button
    v-if="props.data.length > limit"
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
  name: 'LfOrganizationAttributeString',
});

const props = defineProps<{
  data: string,
}>();

const limit = 150;
const showMore = ref<boolean>(false);
</script>
