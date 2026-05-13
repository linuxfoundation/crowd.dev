import { IntegrationConfig } from '@/config/integrations';
import LfDiscourseSettingsDrawer from '@/config/integrations/discourse/components/discourse-settings-drawer.vue';
import DiscourseConnect from './components/discourse-connect.vue';
import DiscourseParams from './components/discourse-params.vue';
import DiscourseDropdown from './components/discourse-dropdown.vue';

const image = new URL('@/assets/images/integrations/discourse.png', import.meta.url).href;

const discourse: IntegrationConfig = {
  key: 'discourse',
  name: 'Discourse',
  image,
  description: 'Sync topics, posts, and replies from your account forums.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations',
  connectComponent: DiscourseConnect,
  connectedParamsComponent: DiscourseParams,
  dropdownComponent: DiscourseDropdown,
  settingComponent: LfDiscourseSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default discourse;
