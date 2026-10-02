import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const GitConnect = defineAsyncComponent(() => import('./components/git-connect.vue'));
const GitDropdown = defineAsyncComponent(() => import('./components/git-dropdown.vue'));
const GitParams = defineAsyncComponent(() => import('./components/git-params.vue'));
const LfGitSettingsDrawer = defineAsyncComponent(
  () => import('./components/git-settings-drawer.vue'),
);

const image = new URL('@/assets/images/integrations/git.png', import.meta.url).href;

const git: IntegrationConfig = {
  key: 'git',
  name: 'Git',
  image,
  description: 'Sync commit activities from Git repositories.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/git-integration',
  connectComponent: GitConnect,
  dropdownComponent: GitDropdown,
  connectedParamsComponent: GitParams,
  settingComponent: LfGitSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default git;
