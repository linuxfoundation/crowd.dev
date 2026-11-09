<template>
  <div v-if="props.data?.length > 0">
    <div class="flex flex-wrap gap-2">
      <component
        :is="props.isLink ? 'a' : 'div'"
        v-for="item of props.data.slice(0, showMore ? props.data.length : limit)"
        :key="item"
        :href="props.isLink ? withHttp(item) : null"
        :target="props.isLink ? '_blank' : null"
      >
        <lf-badge

          type="secondary"
        >
          {{ item }}
        </lf-badge>
      </component>
    </div>

    <lf-button
      v-if="props.data.length > limit"
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
import { ref } from 'vue';
import { withHttp } from '@/utils/string';
import LfButton from '@/ui-kit/button/Button.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';

defineOptions({
  name: 'LfOrganizationAttributeArray',
});

const props = defineProps<{
  data: string[],
  isLink?: boolean,
}>();

const limit = 10;

const showMore = ref<boolean>(false);
</script>
