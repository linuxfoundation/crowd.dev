import type { IntegrationStatusConfig } from '@/modules/admin/modules/integration/config/status/index';

const notConnected: IntegrationStatusConfig = {
  key: 'notConnected',
  show: (integration: any) => !integration || integration.status === 'not-connected',
  statuses: ['not-connected'],
  status: {
    text: 'Not-connected',
    icon: '',
    color: 'text-gray-600',
  },
  actionBar: {
    background: 'bg-gray-50',
    color: 'text-gray-900',
  },
  tabs: {
    text: 'Not connected',
    empty: 'No integrations to be connected',
    badge: 'bg-gray-100',
  },
  chipStatus: {
    icon: 'link-simple-slash',
    iconType: 'light',
    color: 'text-gray-600',
  },
};

export default notConnected;
