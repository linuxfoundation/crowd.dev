import { IntegrationConfig } from '@/config/integrations';
import LfRedditSettingsDrawer from '@/config/integrations/reddit/components/reddit-settings-drawer.vue';
import RedditConnect from './components/reddit-connect.vue';
import RedditParams from './components/reddit-params.vue';
import RedditDropdown from './components/reddit-dropdown.vue';

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
