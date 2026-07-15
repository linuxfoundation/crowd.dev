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

<script>
import { mapActions } from 'vuex';
import AppResizePage from '@/modules/layout/pages/resize-page.vue';
import { storeToRefs } from 'pinia';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import useSessionTracking from '@/shared/modules/monitoring/useSessionTracking';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfGlobals from '@/shared/components/globals.vue';
import { VueQueryDevtools } from '@tanstack/vue-query-devtools';
import ToastContainer from '@/shared/message/toaster-container.vue';

export default {
  name: 'App',

  components: {
    LfGlobals,
    AppResizePage,
    VueQueryDevtools,
    ToastContainer,
  },

  setup() {
    const authStore = useAuthStore();
    const { detachListeners } = useSessionTracking();
    const { listProjectGroups } = useLfSegmentsStore();
    const { init } = authStore;
    const { tenant, loaded } = storeToRefs(authStore);
    return {
      init, tenant, loaded, detachListeners, listProjectGroups,
    };
  },

  watch: {
    tenant: {
      handler(tenant, oldTenant) {
        if (tenant?.id && tenant.id !== oldTenant?.id) {
          this.listProjectGroups({
            limit: 20,
            reset: true,
          });
        }
      },
    },
  },

  async created() {
    window.addEventListener('resize', this.handleResize);
    this.handleResize();
    const queryParameters = new URLSearchParams(window.location.search);
    if (queryParameters.get('state') === 'noconnect' && window.location.pathname.includes('/integration')) {
      return;
    }
    if (['/auth/callback'].includes(window.location.pathname)) {
      return;
    }
    this.init();
  },

  unmounted() {
    this.detachListeners();
    window.removeEventListener('resize', this.handleResize);
  },

  methods: {
    ...mapActions({
      resize: 'layout/resize',
    }),

    handleResize() {
      this.resize({
        width: window.innerWidth,
        height: window.innerHeight,
      });
    },
  },
};
</script>

<style lang="scss">
@import 'assets/scss/index.scss';

.app-page-spinner.custom .el-loading-spinner .circular {
  height: 12rem;
  width: 12rem;
}
</style>
