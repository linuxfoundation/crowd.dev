import type { GitHubSettings } from '@/config/integrations/github-nango/types/GithubSettings';

const repoKey = (owner: string, repoName: string) => `${owner}/${repoName}`.toLowerCase();

const repoKeyFromUrl = (url: string) => {
  const [repoName, owner] = url.replace(/\/+$/, '').split('/').reverse();
  return repoKey(owner ?? '', repoName ?? '');
};

export const githubRepoConnectionMatcher = (settings: GitHubSettings) => {
  const syncedUrls = new Set((settings.syncedRepos ?? []).map((url) => url.toLowerCase()));
  const nangoRepoKeys = new Set(
    Object.values(settings.nangoMapping ?? {}).map((repo) => repoKey(repo.owner, repo.repoName)),
  );

  return (url: string): boolean =>
    syncedUrls.has(url.toLowerCase()) || nangoRepoKeys.has(repoKeyFromUrl(url));
};
