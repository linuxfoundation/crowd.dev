import { IntegrationConfig } from '@/config/integrations';
import SchedConnect from './components/sched-connect.vue';
import SchedParams from './components/sched-params.vue';
import SchedDropdown from './components/sched-dropdown.vue';
import LfSchedSettingsDrawer from './components/sched-settings-drawer.vue';

const image = new URL('@/assets/images/integrations/cvent.png', import.meta.url).href;

const sched: IntegrationConfig = {
  key: 'sched',
  name: 'Sched',
  image,
  description: 'Sync Sched events speakers and attendees data.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations/sched',
  connectComponent: SchedConnect,
  connectedParamsComponent: SchedParams,
  dropdownComponent: SchedDropdown,
  settingComponent: LfSchedSettingsDrawer,
  showProgress: false,
};

export default sched;
