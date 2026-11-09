<template>
  <div>
    <div class="flex flex-wrap gap-2">
      <lf-badge v-for="tag of props.data.slice(0, showMore ? props.data.length : 10)" :key="tag" type="secondary">
        {{ tag }}
      </lf-badge>
    </div>

    <lf-button
      v-if="props.data.length > 10"
      type="primary-link"
      size="small"
      class="mt-3"
      @click="showMore = !showMore"
    >
      Show {{ showMore ? 'less' : 'more' }}
    </lf-button>
  </div>
</template>

<script setup lang="ts">
import LfBadge from '@/ui-kit/badge/Badge.vue';
import { ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';

defineOptions({
  name: 'LfContributorAttributeTags',
});

const props = defineProps<{
  data: string[],
}>();

const showMore = ref<boolean>(false);
</script>
