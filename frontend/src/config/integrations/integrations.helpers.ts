import type { IntegrationConfig } from '@/config/integrations/index';
import { lfIntegrations } from '@/config/integrations/index';
import { useIntegrationStore } from '@/modules/integration/store/integration.store';

const useIntegrationsHelpers = () => {
  const getActiveIntegrations = (): IntegrationConfig[] => {
    const integrations = useIntegrationStore().array;

    return Object.values(lfIntegrations()).filter((config) =>
      integrations.some(
        (integration) => integration.platform === config.key && !!integration.status,
      ),
    );
  };

  return {
    getActiveIntegrations,
  };
};

export default useIntegrationsHelpers;
