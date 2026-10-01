<template>
  <div class="flex flex-col justify-center py-20 items-center">
    <lf-icon :name="icon" class="text-gray-300" :size="120" />
    <span class="text-sm text-gray-500 mt-8">Page not found</span>
    <span class="text-gray-900 text-lg font-semibold mt-3">{{ message }}</span>
    <lf-button type="primary" class="mt-8" @click="goBack">
      {{ backMessage }}
    </lf-button>
  </div>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { useRouter } from 'vue-router';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';

const props = withDefaults(
  defineProps<{
    icon: string;
    message: string;
    backMessage: string;
    to: Record<string, unknown>;
    projectGroup?: boolean;
  }>(),
  {
    icon: 'eyes',
    message: 'Oops! The page you are looking for doesn’t exist.',
    backMessage: 'Back to people',
    projectGroup: true,
  },
);
const router = useRouter();
const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const goBack = () => {
  const { back } = router.options.history.state;
  if (!back || back.toString().includes('/auth')) {
    router.push(customURL());
    return;
  }
  router.go(-1);
};

const customURL = () => ({
  ...props.to,
  query: {
    projectGroup: props.projectGroup
      ? selectedProjectGroup.value?.id
      : undefined,
    ...(props.to.query || {}),
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfNotFoundPage',
};
</script>
