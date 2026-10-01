import type { IntegrationStatusConfig } from '@/modules/admin/modules/integration/config/status/index';

const waitingForAction: IntegrationStatusConfig = {
  key: 'waitingForAction',
  show: (integration: any) => ['pending-action', 'mapping'].includes(integration.status),
  statuses: ['pending-action', 'mapping'],
  status: {
    text: 'Action required',
    icon: 'triangle-exclamation',
    iconType: 'solid',
    color: 'text-yellow-600',
  },
  actionBar: {
    background: 'bg-yellow-50',
    color: 'text-yellow-900',
  },
  tabs: {
    text: 'Action required',
    empty: 'No integrations with a pending action',
    badge: 'bg-yellow-100',
  },
  chipStatus: {
    icon: 'triangle-exclamation',
    iconType: 'solid',
    color: 'text-yellow-500',
  },
};

export default waitingForAction;
