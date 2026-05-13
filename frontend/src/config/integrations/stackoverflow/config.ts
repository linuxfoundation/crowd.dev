import { IntegrationConfig } from '@/config/integrations';
import LfStackoverflowSettingsDrawer from '@/config/integrations/stackoverflow/components/stackoverflow-settings-drawer.vue';
import StackoverflowConnect from './components/stackoverflow-connect.vue';
import StackoverflowDropdown from './components/stackoverflow-dropdown.vue';
import StackoverflowParams from './components/stackoverflow-params.vue';

const image = new URL('@/assets/images/integrations/stackoverflow.png', import.meta.url).href;

const stackoverflow: IntegrationConfig = {
  key: 'stackoverflow',
  name: 'Stack Overflow',
  image,
  description: 'Sync questions and answers based on selected tags.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/stack-overflow',
  connectComponent: StackoverflowConnect,
  dropdownComponent: StackoverflowDropdown,
  connectedParamsComponent: StackoverflowParams,
  settingComponent: LfStackoverflowSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default stackoverflow;
