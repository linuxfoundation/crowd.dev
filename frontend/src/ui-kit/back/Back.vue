<template>
  <div @click="goBack()">
    <slot />
  </div>
</template>

<script setup lang="ts">
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import { useRouter } from 'vue-router';

defineOptions({
  name: 'LfBack',
});

const props = withDefaults(defineProps<{
  to: Record<string, unknown>,
  projectGroup?: boolean,
}>(), {
  projectGroup: true,
});

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const router = useRouter();

const customURL = () => ({
  ...props.to,
  query: {
    projectGroup: props.projectGroup ? selectedProjectGroup.value?.id : undefined,
    ...(props.to.query || {}),
  },
});

const goBack = () => {
  const { back } = router.options.history.state;
  if (!back || back.toString().includes('/auth')) {
    router.push(customURL());
    return;
  }
  router.go(-1);
};

</script>
