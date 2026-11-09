<template>
  <div class="py-10 h-screen w-full flex flex-col items-center justify-center">
    <section class="flex justify-center items-center pb-10">
      <div
        class="w-20 h-20 bg-white shadow-md rounded-full flex items-center justify-center mr-4"
      >
        <lf-svg name="git-repository" class="w-10 h-10" />
      </div>
      <div class="flex items-center gap-2">
        <div class="w-1 h-1 rounded-full bg-gray-300" />
        <div class="w-1 h-1 rounded-full bg-gray-300" />
        <div class="w-1 h-1 rounded-full bg-gray-300" />
        <div class="w-1 h-1 rounded-full bg-gray-300" />
      </div>
      <div class="px-2">
        <lf-icon
          name="circle-check"
          type="solid"
          :size="20"
          class="text-green-500"
        />
      </div>
      <div class="flex items-center gap-2">
        <div class="w-1 h-1 rounded-full bg-gray-300" />
        <div class="w-1 h-1 rounded-full bg-gray-300" />
        <div class="w-1 h-1 rounded-full bg-gray-300" />
        <div class="w-1 h-1 rounded-full bg-gray-300" />
      </div>
      <div
        class="w-20 h-20 bg-white shadow-md rounded-full flex items-center justify-center ml-4"
      >
        <div>
          <img alt="LFX logo" :src="lfxImage" />
        </div>
      </div>
    </section>
    <section class="flex flex-col items-center">
      <h4 class="text-center pb-10">
        LFX CM successfully installed
      </h4>
      <p class="text-medium text-center pb-10 max-w-148">
        To finalize the process of connecting this GitHub organization to LFX
        Community Data Platform, each repository must be mapped to its
        corresponding sub-project.
      </p>
      <div
        class="bg-primary-50 border border-primary-100 flex items-center px-3 py-1.5 rounded-md"
      >
        <lf-icon name="circle-info" :size="20" class="text-primary-800" />
        <p class="ml-2 text-small text-primary-800">
          You must have a LF account and permissions to access Community
          Management in order to complete this final step.
        </p>
      </div>
    </section>
  </div>
</template>

<script lang="ts" setup>
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSvg from '@/shared/svg/svg.vue';

defineOptions({
  name: 'LfIntegrationSuccessPage',
});

const lfxImage = new URL('@/assets/images/logo/lfx.svg', import.meta.url).href;
</script>
