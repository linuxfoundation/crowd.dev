import { IntegrationConfig } from '@/config/integrations';
import GitlabConnect from './components/gitlab-connect.vue';
import GitlabParams from './components/gitlab-params.vue';
import GitlabAction from './components/gitlab-action.vue';
import GitlabStatus from './components/gitlab-status.vue';

const image = new URL('@/assets/images/integrations/gitlab.png', import.meta.url).href;

const gitlab: IntegrationConfig = {
  key: 'gitlab',
  name: 'GitLab',
  image,
  description: 'Sync profile information, merge requests, issues, and more.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations',
  connectComponent: GitlabConnect,
  connectedParamsComponent: GitlabParams,
  actionComponent: GitlabAction,
  statusComponent: GitlabStatus,
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

export default gitlab;
