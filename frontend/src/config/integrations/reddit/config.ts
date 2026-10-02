import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const LfRedditSettingsDrawer = defineAsyncComponent(
  () => import('@/config/integrations/reddit/components/reddit-settings-drawer.vue'),
);
const RedditConnect = defineAsyncComponent(() => import('./components/reddit-connect.vue'));
const RedditParams = defineAsyncComponent(() => import('./components/reddit-params.vue'));
const RedditDropdown = defineAsyncComponent(() => import('./components/reddit-dropdown.vue'));

const image = new URL('@/assets/images/integrations/reddit.svg', import.meta.url).href;

const reddit: IntegrationConfig = {
  key: 'reddit',
  name: 'Reddit',
  image,
  description: 'Sync posts and comments from selected subreddits.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/reddit-integration',
  connectComponent: RedditConnect,
  connectedParamsComponent: RedditParams,
  dropdownComponent: RedditDropdown,
  settingComponent: LfRedditSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default reddit;
