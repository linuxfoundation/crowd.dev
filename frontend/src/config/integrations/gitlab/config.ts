import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const GitlabConnect = defineAsyncComponent(() => import('./components/gitlab-connect.vue'));
const GitlabParams = defineAsyncComponent(() => import('./components/gitlab-params.vue'));
const GitlabAction = defineAsyncComponent(() => import('./components/gitlab-action.vue'));
const GitlabStatus = defineAsyncComponent(() => import('./components/gitlab-status.vue'));

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
