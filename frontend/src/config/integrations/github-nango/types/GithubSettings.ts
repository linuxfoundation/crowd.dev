export interface GitHubOrganization {
  logo: string;
  name: string;
  url: string;
  updatedAt?: string;
}

export interface GitHubRepository {
  name: string;
  url: string;
  forkedFrom?: string | null;
  org?: GitHubOrganization;
}

export interface GitHubSettingsRepository extends GitHubRepository {
  updatedAt?: string;
}

export interface GitHubSettingsOrganization extends GitHubOrganization {
  fullSync: boolean;
  repos: GitHubSettingsRepository[];
}
export interface GitHubSettings {
  orgs: GitHubSettingsOrganization[];
  nangoMapping?: Record<string, { repoName: string }>;
  updateMemberAttributes: boolean;
}
