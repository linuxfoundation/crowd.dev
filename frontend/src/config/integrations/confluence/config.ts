import { IntegrationConfig } from '@/config/integrations';
import ConfluenceConnect from './components/confluence-connect.vue';
import ConfluenceParams from './components/confluence-params.vue';
import ConfluenceDropdown from './components/confluence-dropdown.vue';
import LfConfluenceSettingsDrawer from './components/confluence-settings-drawer.vue';

const image = new URL('@/assets/images/integrations/confluence.svg', import.meta.url).href;

const confluence: IntegrationConfig = {
  key: 'confluence',
  name: 'Confluence',
  image,
  description: 'Sync documentation activities from your spaces.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/confluence',
  connectComponent: ConfluenceConnect,
  connectedParamsComponent: ConfluenceParams,
  dropdownComponent: ConfluenceDropdown,
  settingComponent: LfConfluenceSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default confluence;
