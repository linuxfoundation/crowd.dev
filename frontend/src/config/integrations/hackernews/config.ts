import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const HackernewsConnect = defineAsyncComponent(() => import('./components/hackernews-connect.vue'));
const HackernewsParams = defineAsyncComponent(() => import('./components/hackernews-params.vue'));

const image = new URL('@/assets/images/integrations/hackernews.svg', import.meta.url).href;

const hackernews: IntegrationConfig = {
  key: 'hackernews',
  name: 'Hacker News',
  image,
  description: 'Sync posts and comments mentioning your community.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/hacker-news-integration',
  connectComponent: HackernewsConnect,
  connectedParamsComponent: HackernewsParams,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default hackernews;
