import type { Component } from 'vue';
import Layout from '@/modules/layout/components/layout.vue';
import routes from '@/modules/layout/layout-routes';

const components: Record<string, Component> = {
  'app-layout': Layout,
};

export default {
  components,
  routes,
};
