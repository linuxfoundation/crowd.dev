import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const LfDiscourseSettingsDrawer = defineAsyncComponent(
  () => import('@/config/integrations/discourse/components/discourse-settings-drawer.vue'),
);
const DiscourseConnect = defineAsyncComponent(() => import('./components/discourse-connect.vue'));
const DiscourseParams = defineAsyncComponent(() => import('./components/discourse-params.vue'));
const DiscourseDropdown = defineAsyncComponent(() => import('./components/discourse-dropdown.vue'));

const image = new URL('@/assets/images/integrations/discourse.png', import.meta.url).href;

const discourse: IntegrationConfig = {
  key: 'discourse',
  name: 'Discourse',
  image,
  description: 'Sync topics, posts, and replies from your account forums.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations',
  connectComponent: DiscourseConnect,
  connectedParamsComponent: DiscourseParams,
  dropdownComponent: DiscourseDropdown,
  settingComponent: LfDiscourseSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default discourse;
