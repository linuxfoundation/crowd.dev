import authAxios from '@/shared/axios/auth-axios';
import type {
  GitHubOrganization,
  GitHubRepository,
} from '@/config/integrations/github-nango/types/GithubSettings';
import type { Pagination } from '@/shared/types/Pagination';

export class GithubApiService {
  static async searchRepositories(
    query: string,
    offset: number = 0,
    limit: number = 20,
  ): Promise<Pagination<GitHubRepository>> {
    const response = await authAxios.get('/integration/github/search/repos', {
      params: {
        query,
        offset,
        limit,
      },
    });

    return response.data;
  }

  static async searchOrganizations(
    query: string,
    offset: number = 0,
    limit: number = 20,
  ): Promise<Pagination<GitHubOrganization>> {
    const response = await authAxios.get('/integration/github/search/orgs', {
      params: {
        query,
        offset,
        limit,
      },
    });

    return response.data;
  }

  static async getOrganizationRepositories(
    name: string,
  ): Promise<GitHubRepository[]> {
    const response = await authAxios.get(
      `/integration/github/orgs/${name}/repos`,
    );

    return response.data;
  }
}
