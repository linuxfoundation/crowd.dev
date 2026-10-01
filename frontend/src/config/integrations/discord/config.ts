import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const DiscordConnect = defineAsyncComponent(() => import('./components/discord-connect.vue'));
const DiscordParams = defineAsyncComponent(() => import('./components/discord-params.vue'));

const image = new URL('@/assets/images/integrations/discord.png', import.meta.url).href;

const discord: IntegrationConfig = {
  key: 'discord',
  name: 'Discord',
  image,
  description: 'Sync messages, threads, forum channels, and new joiners.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/discord-integration',
  connectComponent: DiscordConnect,
  connectedParamsComponent: DiscordParams,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default discord;
