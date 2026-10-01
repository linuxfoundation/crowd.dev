import { defineAsyncComponent } from 'vue';
import type { IntegrationConfig } from '@/config/integrations';

const LfJiraSettingsDrawer = defineAsyncComponent(
  () => import('@/config/integrations/jira/components/jira-settings-drawer.vue'),
);
const JiraConnect = defineAsyncComponent(() => import('./components/jira-connect.vue'));
const JiraParams = defineAsyncComponent(() => import('./components/jira-params.vue'));
const JiraDropdown = defineAsyncComponent(() => import('./components/jira-dropdown.vue'));

const image = new URL('@/assets/images/integrations/jira.png', import.meta.url).href;

const jira: IntegrationConfig = {
  key: 'jira',
  name: 'Jira',
  image,
  description: 'Sync issues activities from your projects.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations',
  connectComponent: JiraConnect,
  connectedParamsComponent: JiraParams,
  dropdownComponent: JiraDropdown,
  settingComponent: LfJiraSettingsDrawer,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default jira;
