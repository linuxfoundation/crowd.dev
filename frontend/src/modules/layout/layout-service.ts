import type { Status } from '@/modules/layout/types/SystemStatus';
import authAxios from '@/shared/axios/auth-axios';

export default {
  async getSystemStatus(): Promise<{ status: Status }> {
    const response = await authAxios.get('/system-status');

    return response.data;
  },
};
