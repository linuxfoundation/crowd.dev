import { IntegrationConfig } from '@/config/integrations';
import LfGerritSettingsDrawer from '@/config/integrations/gerrit/components/gerrit-settings-drawer.vue';
import GerritConnect from './components/gerrit-connect.vue';
import GerritParams from './components/gerrit-params.vue';
import GerritDropdown from './components/gerrit-dropdown.vue';

const image = new URL('@/assets/images/integrations/gerrit.png', import.meta.url).href;

const gerrit: IntegrationConfig = {
  key: 'gerrit',
  name: 'Gerrit',
  image,
  description: 'Sync documentation activities from Gerrit repositories.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/gerrit',
  connectComponent: GerritConnect,
  connectedParamsComponent: GerritParams,
  dropdownComponent: GerritDropdown,
  settingComponent: LfGerritSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default gerrit;
