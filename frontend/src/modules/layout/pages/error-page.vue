<template>
  <div
    class="flex items-center justify-between h-screen col-start-1 col-end-13 py-16 relative max-w-5xl mx-auto"
  >
    <div class="content">
      <div class="text-primary-500 text-xs uppercase font-semibold">
        Error {{ code }}
      </div>
      <div class="text-gray-900 font-semibold mt-2 text-3xl">
        {{ title }}
      </div>
      <div class="text-gray-600 mt-2">
        {{ subtitle }}
      </div>
      <div class="mt-10">
        <router-link :to="{ path: '/project-groups' }">
          <lf-button type="primary" size="medium">
            Back to Home
          </lf-button>
        </router-link>
      </div>
    </div>
    <img :src="imageUrl(code)" alt="404 img" class="block w-32 h-auto" />
    <div class="absolute right-0 bottom-0 mb-16 flex items-center">
      <a
        href="https://jira.linuxfoundation.org/plugins/servlet/desk/portal/4?requestGroup=54"
        class="btn btn-link btn-link--md btn-link--primary flex items-center mr-6"
        target="_blank"
        rel="noopener noreferrer"
      >
        <span class="leading-4"> Open an issue</span>
      </a>
      <a
        href="https://app.slack.com/client/T02H1G4T9/C0DMD0214"
        class="btn btn-link btn-link--md btn-link--primary flex items-center"
        target="_blank"
        rel="noopener noreferrer"
      >
        <span class="leading-4">Slack</span>
      </a>
    </div>
  </div>
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';

withDefaults(
  defineProps<{
    code?: number | null;
    title?: string | null;
    subtitle?: string | null;
  }>(),
  {
    code: null,
    title: null,
    subtitle: null,
  },
);

const imageUrl = (code: number | null) => {
  switch (code) {
    case 403:
      return new URL('@/assets/images/error/403.svg', import.meta.url).href;
    case 500:
      return new URL('@/assets/images/error/500.svg', import.meta.url).href;
    default:
      return new URL('@/assets/images/error/404.svg', import.meta.url).href;
  }
};
</script>

<script lang="ts">
export default {
  name: 'AppError404Page',
};
</script>
