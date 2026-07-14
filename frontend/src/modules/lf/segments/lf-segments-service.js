import authAxios from '@/shared/axios/auth-axios';
import { AuthService } from '@/modules/auth/services/auth.service';

export class LfService {
  // Segments

  static async findSegment(id) {
    const response = await authAxios.get(
      `/segment/${id}`,
      {
        params: {
          segments: [id],
        },
      },
    );

    return response.data;
  }

  static async listSegmentsByIds(ids) {
    const response = await authAxios.post(
      '/segment/id',
      {
        ids,
      },
    );

    return response.data;
  }

  static async updateSegment(id, data) {
    const response = await authAxios.put(
      `/segment/${id}`,
      {
        ...data,
        segments: [id],
      },
    );

    return response.data;
  }

  // Project Groups

  static async queryProjectGroups(body) {
    const response = await authAxios.post(
      '/segment/projectGroup/query',
      {
        ...body,
        excludeSegments: true,
      },
    );

    return response.data;
  }

  static async createProjectGroup(body) {
    const response = await authAxios.post(
      '/segment/projectGroup',
      {
        ...body,
        excludeSegments: true,
      },
    );

    return response.data;
  }

  // Projects

  static async queryProjects(body) {
    const response = await authAxios.post(
      '/segment/project/query',
      {
        ...body,
        ...(body.segments ? { segments: body.segments } : { excludeSegments: true }),
      },
    );

    return response.data;
  }

  static async createProject(body) {
    const response = await authAxios.post(
      '/segment/project',
      body,
    );

    return response.data;
  }

  static async querySubprojects(body) {
    const response = await authAxios.post(
      '/segment/subproject/query',
      {
        ...body,
        excludeSegments: true,
      },
    );

    return response.data;
  }

  // Sub-projects

  static async createSubProject(body) {
    const response = await authAxios.post(
      '/segment/subproject',
      body,
    );

    return response.data;
  }

  // Users

  static async fetchUsers(query, limit) {
    const params = {
      query,
      limit,
    };

    const response = await authAxios.get(
      '/user/autocomplete',
      {
        params,
      },
    );

    return response.data;
  }

  static async getUser(id) {
    const response = await authAxios.get(
      `/user/${id}`,
    );

    return response.data;
  }

  // AuditLogs

  static async fetchAuditLogs(data) {
    const response = await authAxios.post(
      '/audit-logs/query',
      data,
    );

    return response.data;
  }
}
