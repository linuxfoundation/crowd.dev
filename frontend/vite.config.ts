import { fileURLToPath, URL } from 'node:url';
import dns from 'node:dns';

import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

import Components from 'unplugin-vue-components/vite';
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers';

// https://vite.dev/config/server-options#server-host
dns.setDefaultResultOrder('verbatim');

const analyzerPlugins = async () => {
  if (!process.env.ANALYZE) return [];
  // rollup-plugin-visualizer is ESM-only, so a static import breaks this CJS-loaded config
  const { visualizer } = await import('rollup-plugin-visualizer');
  return [
    visualizer({
      template: 'treemap',
      open: false,
      gzipSize: true,
      brotliSize: true,
      filename: 'analyse.html',
    }),
  ];
};

export default defineConfig(async () => ({
  envPrefix: 'VUE_APP',
  plugins: [
    vue(),
    Components({
      extensions: ['vue', 'md'],
      include: [/\.vue$/, /\.vue\?vue/, /\.md$/],
      resolvers: [
        ElementPlusResolver({
          importStyle: 'sass',
        }),
      ],
      dts: 'components.d.ts',
    }),
    ...(await analyzerPlugins()),
  ],
  resolve: {
    extensions: ['.mjs', '.js', '.ts', '.jsx', '.tsx', '.json', '.vue', '.svg', '.png'],
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
      '~': fileURLToPath(new URL('./node_modules', import.meta.url)),
    },
  },
  server: {
    proxy: {
      '/api': {
        target: process.env.BACKEND_URL || 'http://localhost:8080',
        rewrite: (path: string) => path.replace(/^\/api/, ''),
      },
    },
    port: 8081,
    host: true,
  },
}));
