<template>
  <div class="grid grid-cols-12 h-full">
    <div class="flex items-center justify-between col-start-2 col-end-12">
      <div class="w-2/3">
        <div class="text-gray-900 font-semibold mt-2 text-3xl">
          Upgrade plan
        </div>
        <div class="text-gray-600 mt-2">
          You need to upgrade your plan in order to enable this feature. Please
          leave us a message.
        </div>
        <div class="mt-10">
          <a
            href="https://www.crowd.dev/contact"
            target="_blank"
            rel="noopener noreferrer"
          >
            <lf-button type="primary" size="medium" @click="onCtaClick">
              Contact us
            </lf-button>
          </a>
        </div>s
      </div>
      <img :src="errorImage" alt="403 img" class="block w-32 h-auto" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRoute } from 'vue-router';
import LfButton from '@/ui-kit/button/Button.vue';

defineOptions({ name: 'AppPaywallPage' });

const route = useRoute();

const onCtaClick = () => {
  window.analytics.track('Upgrade Plan Contact', {
    route: route.path,
  });
};

const errorImage = new URL('@/assets/images/error/403.svg', import.meta.url)
  .href;
</script>
