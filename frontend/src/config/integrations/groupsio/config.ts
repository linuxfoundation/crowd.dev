import { IntegrationConfig } from '@/config/integrations';
import GroupsioConnect from './components/groupsio-connect.vue';
import GroupsioParams from './components/groupsio-params.vue';
import GroupsioDropdown from './components/groupsio-dropdown.vue';
import LfGroupsioSettingsDrawer from './components/groupsio-settings-drawer.vue';

const image = new URL('@/assets/images/integrations/groupsio.svg', import.meta.url).href;

const groupsio: IntegrationConfig = {
  key: 'groupsio',
  name: 'Groups.io',
  image,
  description: 'Sync groups and topics activity.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/groups.io',
  connectComponent: GroupsioConnect,
  connectedParamsComponent: GroupsioParams,
  dropdownComponent: GroupsioDropdown,
  settingComponent: LfGroupsioSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default groupsio;
