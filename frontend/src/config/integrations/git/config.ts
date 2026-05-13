import { IntegrationConfig } from '@/config/integrations';
import GitConnect from './components/git-connect.vue';
import GitDropdown from './components/git-dropdown.vue';
import GitParams from './components/git-params.vue';
import LfGitSettingsDrawer from './components/git-settings-drawer.vue';

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
