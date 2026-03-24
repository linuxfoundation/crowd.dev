import { IntegrationConfig } from '@/config/integrations';
import LfJiraSettingsDrawer from '@/config/integrations/jira/components/jira-settings-drawer.vue';
import JiraConnect from './components/jira-connect.vue';
import JiraParams from './components/jira-params.vue';
import JiraDropdown from './components/jira-dropdown.vue';

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
