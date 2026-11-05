import authAxios from '@/shared/axios/auth-axios';
import type {
  GlobalIntegrationsQuery,
  GlobalIntegrationStatusCountQuery,
} from '@/modules/admin/modules/users/types/GlobalIntegration';

export class UsersService {
  static async list(query: Record<string, unknown>) {
    const response = await authAxios.get(
      '/user',
      {
        params: query,
      },
    );

    return response.data;
  }

  static async fetchGlobalIntegrations(query: GlobalIntegrationsQuery) {
    const response = await authAxios.get(
      '/integration/global',
      {
        params: query,
      },
    );

    return response.data;
  }

  static async fetchGlobalIntegrationStatusCount(query: GlobalIntegrationStatusCountQuery) {
    const response = await authAxios.get(
      '/integration/global/status',
      {
        params: query,
      },
    );

    return response.data;
  }
}
