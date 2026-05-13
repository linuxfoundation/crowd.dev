import { IntegrationConfig } from '@/config/integrations';
import SlackConnect from './components/slack-connect.vue';
import SlackParams from './components/slack-params.vue';

const image = new URL('@/assets/images/integrations/slack.png', import.meta.url).href;

const slack: IntegrationConfig = {
  key: 'slack',
  name: 'Slack',
  image,
  description: 'Sync messages, threads, and new joiners.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/slack',
  connectComponent: SlackConnect,
  connectedParamsComponent: SlackParams,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default slack;
