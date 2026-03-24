import { IntegrationConfig } from '@/config/integrations';
import LfLinkedinSettingsDrawer from '@/config/integrations/linkedin/components/linkedin-settings-drawer.vue';
import LinkedinConnect from './components/linkedin-connect.vue';
import LinkedinParams from './components/linkedin-params.vue';
import LinkedinAction from './components/linkedin-action.vue';
import LinkedinDropdown from './components/linkedin-dropdown.vue';

const image = new URL('@/assets/images/integrations/linkedin.png', import.meta.url).href;

const linkedin: IntegrationConfig = {
  key: 'linkedin',
  name: 'LinkedIn',
  image,
  description: "Sync comments and reactions from your organization's posts.",
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/linkedin-integration',
  connectComponent: LinkedinConnect,
  connectedParamsComponent: LinkedinParams,
  actionComponent: LinkedinAction,
  dropdownComponent: LinkedinDropdown,
  settingComponent: LfLinkedinSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
    {
      key: 'pending-action',
      text: 'Select the LinkedIn organization to connect.',
    },
  ],
};

export default linkedin;
