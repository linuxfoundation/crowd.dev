<template>
  <div id="app">
    <div class="sm:hidden md:block lg:block xl:block">
      <lfx-header-v2 v-if="!$route.meta.hideLfxHeader" id="lfx-header" product="Community Data Platform" />
      <router-view v-slot="{ Component }">
        <transition>
          <component :is="Component" v-if="Component" />
        </transition>
      </router-view>
      <ToastContainer />

      <div id="teleport-modal" />
    </div>

    <div class="sm:block md:hidden lg:hidden xl:hidden">
      <app-resize-page />
    </div>
    <lf-globals />
    <VueQueryDevtools />
  </div>
</template>

<script setup lang="ts">
import { onUnmounted, watch } from 'vue';
import AppResizePage from '@/modules/layout/pages/resize-page.vue';
import { storeToRefs } from 'pinia';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import useSessionTracking from '@/shared/modules/monitoring/useSessionTracking';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfGlobals from '@/shared/components/globals.vue';
import { VueQueryDevtools } from '@tanstack/vue-query-devtools';
import ToastContainer from '@/shared/message/toaster-container.vue';

defineOptions({ name: 'App' });

const authStore = useAuthStore();
const { detachListeners } = useSessionTracking(() => authStore.user);
const { listProjectGroups } = useLfSegmentsStore();
const { init } = authStore;
const { tenant } = storeToRefs(authStore);

watch(tenant, (newTenant, oldTenant) => {
  if (newTenant?.id && newTenant.id !== oldTenant?.id) {
    listProjectGroups({
      limit: 20,
      reset: true,
    });
  }
});

const queryParameters = new URLSearchParams(window.location.search);
const isNoConnectIntegration = queryParameters.get('state') === 'noconnect'
  && window.location.pathname.includes('/integration');
const isAuthCallback = ['/auth/callback'].includes(window.location.pathname);

if (!isNoConnectIntegration && !isAuthCallback) {
  init();
}

onUnmounted(() => {
  detachListeners();
});
</script>

<style lang="scss">
@import 'assets/scss/index.scss';

.app-page-spinner.custom .el-loading-spinner .circular {
  height: 12rem;
  width: 12rem;
}
</style>
