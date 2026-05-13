import { IntegrationConfig } from '@/config/integrations';
import DevtoConnect from './components/devto-connect.vue';
import DevtoParams from './components/devto-params.vue';

const image = new URL('@/assets/images/integrations/devto.png', import.meta.url).href;

const devto: IntegrationConfig = {
  key: 'devto',
  name: 'DEV',
  image,
  description: 'Sync profile information and comments on articles.',
  link: 'https://docs.linuxfoundation.org/lfx/community-management/integrations',
  connectComponent: DevtoConnect,
  connectedParamsComponent: DevtoParams,
  showProgress: false,
  actionRequiredMessage: [
    {
      key: 'needs-reconnect',
      text: 'Reconnect your account to restore access.',
    },
  ],
};

export default devto;
