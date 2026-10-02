import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const LfGerritSettingsDrawer = defineAsyncComponent(
  () => import('@/config/integrations/gerrit/components/gerrit-settings-drawer.vue'),
);
const GerritConnect = defineAsyncComponent(() => import('./components/gerrit-connect.vue'));
const GerritParams = defineAsyncComponent(() => import('./components/gerrit-params.vue'));
const GerritDropdown = defineAsyncComponent(() => import('./components/gerrit-dropdown.vue'));

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
