import { IntegrationConfig } from '@/config/integrations';
import GithubConnect from './components/github-connect.vue';
import GithubStatus from './components/github-status.vue';
import GithubAction from './components/github-action.vue';
import GithubParams from './components/github-params.vue';
import GithubMappedRepos from './components/github-mapped-repos.vue';

const image = new URL('@/assets/images/integrations/github.png', import.meta.url).href;

const github: IntegrationConfig = {
  key: 'github',
  name: 'GitHub',
  image,
  description: 'Sync profile information, stars, forks, pull requests, issues, and discussions.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/github-integration',
  connectComponent: GithubConnect,
  statusComponent: GithubStatus,
  actionComponent: GithubAction,
  connectedParamsComponent: GithubParams,
  mappedReposComponent: GithubMappedRepos,
  showProgress: true,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
    {
      key: 'mapping',
      text: 'Select repositories to track and map them to projects.',
    },
    {
      key: 'waiting-approval',
      text: 'Waiting for organization admin to approve the installation.',
    },
  ],
};

export default github;
