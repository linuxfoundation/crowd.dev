<template>
  <div
    class="mt-1"
  >
    <div
      v-for="[key, value] of dataList.slice(0, showMore ? dataList.length : limit)"
      :key="key"
      class="text-xs"
    >
      <span
        class="flex flex-grow items-center justify-between"
      >
        <span class="font-medium mr-1 text-gray-900">{{ key }}</span>
        <span class="text-gray-600">{{ value }}</span>
      </span>
    </div>
  </div>
  <lf-button
    v-if="dataList.length > limit"
    type="primary-link"
    size="small"
    class="mt-3"
    @click="showMore = !showMore"
  >
    Show {{ showMore ? 'less' : 'more' }}
  </lf-button>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';

defineOptions({
  name: 'LfOrganizationAttributeJson',
});

const props = defineProps<{
  data: Record<string, string>
}>();

const limit = 10;

const showMore = ref(false);

const dataList = computed(() => Object.entries(props.data));
</script>
