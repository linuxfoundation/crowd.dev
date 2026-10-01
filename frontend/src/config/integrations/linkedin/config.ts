import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const LfLinkedinSettingsDrawer = defineAsyncComponent(
  () => import('@/config/integrations/linkedin/components/linkedin-settings-drawer.vue'),
);
const LinkedinConnect = defineAsyncComponent(() => import('./components/linkedin-connect.vue'));
const LinkedinParams = defineAsyncComponent(() => import('./components/linkedin-params.vue'));
const LinkedinAction = defineAsyncComponent(() => import('./components/linkedin-action.vue'));
const LinkedinDropdown = defineAsyncComponent(() => import('./components/linkedin-dropdown.vue'));

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
