import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const LfTwitterSettingsDrawer = defineAsyncComponent(
  () => import('@/config/integrations/twitter/components/twitter-settings-drawer.vue'),
);
const TwitterConnect = defineAsyncComponent(() => import('./components/twitter-connect.vue'));
const TwitterParams = defineAsyncComponent(() => import('./components/twitter-params.vue'));
const TwitterDropdown = defineAsyncComponent(() => import('./components/twitter-dropdown.vue'));

const image = new URL('@/assets/images/integrations/twitter-x-black.png', import.meta.url).href;

const twitter: IntegrationConfig = {
  key: 'twitter',
  name: 'X/Twitter',
  image,
  description: 'Sync profile information, followers, and relevant tweets.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/x-twitter-integration',
  connectComponent: TwitterConnect,
  connectedParamsComponent: TwitterParams,
  dropdownComponent: TwitterDropdown,
  settingComponent: LfTwitterSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default twitter;
