import type { GitHubSettings } from '@/config/integrations/github-nango/types/GithubSettings';

export const githubRepoConnectionMatcher = (settings: GitHubSettings) => {
  const syncedUrls = new Set((settings.syncedRepos ?? []).map((url) => url.toLowerCase()));

  return (url: string): boolean => syncedUrls.has(url.toLowerCase());
};
