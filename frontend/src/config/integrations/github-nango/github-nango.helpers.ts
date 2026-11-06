import type { GitHubSettings } from '@/config/integrations/github-nango/types/GithubSettings';

const repoNameFromUrl = (url: string) => url.split('/').at(-1) as string;

export const githubRepoConnectionMatcher = (settings: GitHubSettings) => {
  const syncedUrls = new Set((settings.syncedRepos ?? []).map((url) => url.toLowerCase()));
  const nangoRepoNames = new Set(
    Object.values(settings.nangoMapping ?? {}).map((repo) => repo.repoName),
  );

  return (url: string): boolean =>
    syncedUrls.has(url.toLowerCase()) || nangoRepoNames.has(repoNameFromUrl(url));
};
