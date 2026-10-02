import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const LfGithubSettingsDrawer = defineAsyncComponent(
  () => import('@/config/integrations/github-nango/components/settings/github-settings-drawer.vue'),
);
// For now we will be referencing the connect component from the github (old) integration
const GithubConnect = defineAsyncComponent(
  () => import('@/config/integrations/github/components/github-connect.vue'),
);
const GithubMappedRepos = defineAsyncComponent(
  () => import('@/config/integrations/github/components/github-mapped-repos.vue'),
);
const GithubParams = defineAsyncComponent(() => import('./components/github-params.vue'));
const GithubDropdown = defineAsyncComponent(() => import('./components/github-dropdown.vue'));

const image = new URL('@/assets/images/integrations/github.png', import.meta.url).href;

const github: IntegrationConfig = {
  key: 'github',
  name: 'GitHub (v2)',
  image,
  description:
    'Sync profile information, star counts, forks, pull requests, issues, and discussions.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/github-integration',
  connectComponent: GithubConnect,
  dropdownComponent: GithubDropdown,
  statusComponent: GithubParams,
  connectedParamsComponent: GithubParams,
  mappedReposComponent: GithubMappedRepos,
  settingComponent: LfGithubSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
    {
      key: 'mapping',
      text: 'Select repositories to track and map them to projects.',
    },
  ],
};

export default github;
