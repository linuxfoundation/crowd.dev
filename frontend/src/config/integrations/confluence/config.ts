import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const ConfluenceConnect = defineAsyncComponent(() => import('./components/confluence-connect.vue'));
const ConfluenceParams = defineAsyncComponent(() => import('./components/confluence-params.vue'));
const ConfluenceDropdown = defineAsyncComponent(
  () => import('./components/confluence-dropdown.vue'),
);
const LfConfluenceSettingsDrawer = defineAsyncComponent(
  () => import('./components/confluence-settings-drawer.vue'),
);

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
