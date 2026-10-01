import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const GroupsioConnect = defineAsyncComponent(() => import('./components/groupsio-connect.vue'));
const GroupsioParams = defineAsyncComponent(() => import('./components/groupsio-params.vue'));
const GroupsioDropdown = defineAsyncComponent(() => import('./components/groupsio-dropdown.vue'));
const LfGroupsioSettingsDrawer = defineAsyncComponent(
  () => import('./components/groupsio-settings-drawer.vue'),
);

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
