import type { IntegrationStatusConfig } from '@/modules/admin/modules/integration/config/status/index';

const connecting: IntegrationStatusConfig = {
  key: 'connecting',
  show: (integration: any) => integration.status === 'in-progress',
  statuses: ['in-progress'],
  status: {
    text: 'Connecting',
    icon: 'circle-notch animate-spin',
    iconType: 'solid',
    color: 'text-secondary-500',
  },
  actionBar: {
    background: 'bg-primary-50',
    color: 'text-secondary-500',
  },
  tabs: {
    text: 'In progress',
    empty: 'No integrations with connection in progress',
    badge: 'bg-primary-50',
  },
  chipStatus: {
    icon: 'clock',
    iconType: 'solid',
    color: 'text-primary-500',
  },
};

export default connecting;
