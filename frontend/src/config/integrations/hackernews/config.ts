import { IntegrationConfig } from '@/config/integrations';
import HackernewsConnect from './components/hackernews-connect.vue';
import HackernewsParams from './components/hackernews-params.vue';

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
