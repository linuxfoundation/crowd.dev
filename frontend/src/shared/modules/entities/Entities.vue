<template>
  <slot :sliced-entities="slicedEntities" />

  <div
    v-if="entities.length > limit"
    class="underline cursor-pointer text-gray-500 hover:text-primary-500 text-xs underline-offset-4"
    @click="displayMore = !displayMore"
  >
    Show {{ displayMore ? "less" : "more" }}
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';

const props = defineProps<{
  entities: unknown[];
  limit: number
}>();

const displayMore = ref(false);

const slicedEntities = computed(() => {
  if (!displayMore.value) {
    return props.entities.slice(0, props.limit);
  }

  return props.entities;
});

</script>

<script lang="ts">
export default {
  name: 'AppEntities',
};
</script>
