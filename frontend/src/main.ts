import { createApp } from 'vue';
import VueClickAway from 'vue3-click-away';
// @ts-ignore
import VueGridLayout from 'vue-grid-layout';
// @ts-ignore
import Vue3Sanitize from 'vue-3-sanitize';
import LogRocketClient from 'logrocket';
import VueLazyLoad from 'vue3-lazyload';
import { createPinia } from 'pinia';
import { createRouter } from '@/router';
import { createStore } from '@/store';
import modules from '@/modules';
import config from '@/config';

import App from '@/app.vue';
import { vueSanitizeOptions } from '@/shared/plugins/sanitize';
import marked from '@/shared/plugins/marked';
import { useLogRocket } from '@/utils/logRocket';
import { initRUM } from '@/utils/datadog/rum';
import { installAnalyticsStub, loadSegment, isRealSegmentKey } from '@/utils/segment';
import { VueQueryPlugin, QueryClient } from '@tanstack/vue-query';

declare module 'vue' {
  interface ComponentCustomProperties {
    $sanitize: (key: string) => string;
  }
}

/**
 * We're using Immediately Invoked Function Expressions (IIFE) here because of the async/awaits
 * (We should probably revisit/refactor this later to be less confusing)
 */
/* eslint-disable no-param-reassign, no-underscore-dangle, func-names */
(async function () {
  installAnalyticsStub();
  if (isRealSegmentKey(config.segmentKey)) {
    loadSegment(config.segmentKey);
  }

  const pinia = createPinia();
  const app = createApp(App);
  app.use(pinia);

  // Create a client
  const queryClient = new QueryClient({
    defaultOptions: { queries: { staleTime: 1000 * 60 * 5 } },
  });

  // Install the VueQuery plugin
  app.use(VueQueryPlugin, {
    queryClient,
  });

  const { captureException } = useLogRocket();

  const router = await createRouter();
  const store = await createStore(LogRocketClient);

  app.use(VueGridLayout);
  app.use(Vue3Sanitize, vueSanitizeOptions);
  app.use(VueClickAway);
  app.use(marked);
  app.use(VueLazyLoad, {});

  app.config.errorHandler = (err: any) => {
    console.error(err);
    if (config.env !== 'local') {
      captureException(err);
    }
  };

  const exists = (el: any) => Boolean(el);
  Object.keys(modules)
    .map((key) => modules[key].components)
    .filter(exists)
    .forEach((components) => {
      Object.keys(components).forEach((name) => {
        app.component(name, components[name]);
      });
    });

  app.use(store).use(router).mount('#app');

  if (config.env === 'production' && config.hotjarKey) {
    (function (h, o, t, j, a, r) {
      // @ts-ignore
      h.hj = h.hj
        // @ts-ignore
        || function (...args) {
          // @ts-ignore
          (h.hj.q = h.hj.q || []).push(args);
        };
      // @ts-ignore
      h._hjSettings = { hjid: config.hotjarKey, hjsv: 6 };
      // @ts-ignore
      [a] = o.getElementsByTagName('head');
      // @ts-ignore
      r = o.createElement('script');
      // @ts-ignore
      r.async = 1;
      // @ts-ignore
      r.src = t + h._hjSettings.hjid + j + h._hjSettings.hjsv;
      // @ts-ignore
      a.appendChild(r);
    }(
      window,
      document,
      'https://static.hotjar.com/c/hotjar-',
      '.js?sv=',
    ));
  }

  if (config.env === 'production' && config.datadog.rum) {
    initRUM();
  }
}());
