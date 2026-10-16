<template>
  <el-container class="flex-col">
    <el-container style="height: calc(100vh - 60px); margin-top: 60px;">
      <!-- App menu -->
      <lf-menu />

      <el-container class="bg-white">
        <el-main id="main-page-wrapper" class="relative flex flex-col">
          <app-lf-banners />
          <router-view />
          <div v-if="footerStore.visible" class="mt-auto mb-1 flex flex-col items-center">
            <lfx-footer class="px-2 max-w-3xl" />
            <lf-button type="secondary-link" size="small" @click="openCookieSettings">
              Cookie settings
            </lf-button>
          </div>
        </el-main>
      </el-container>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import AppLfBanners from '@/modules/lf/layout/components/lf-banners.vue';
import LfMenu from '@/modules/layout/components/menu/menu.vue';
import '@linuxfoundation/lfx-ui-core';
import { useFooterStore } from '@/modules/layout/pinia';
import LfButton from '@/ui-kit/button/Button.vue';
import { openCookieSettings } from '@/utils/transcend';

const footerStore = useFooterStore();
</script>

<script lang="ts">
export default {
  name: 'AppLayout',
};
</script>
