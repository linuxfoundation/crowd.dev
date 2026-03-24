import { InsightsProjectAddFormModel } from './models/insights-project-add-form.model';
import {
  InsightsProjectModel,
  InsightsProjectRequest,
} from './models/insights-project.model';
import { getDefaultWidgets } from './widgets';

export const buildRequest = (
  form: InsightsProjectAddFormModel,
): InsightsProjectRequest => ({
  segmentId: form.segmentId,
  name: form.name,
  slug: form.slug,
  description: form.description,
  logoUrl: form.logoUrl,
  collections: form.collectionsIds,
  organizationId: form.organizationId,
  website: form.website,
  github: form.github,
  twitter: form.twitter,
  linkedin: form.linkedin,
  repositories: form.repositories?.filter((r) => r.enabled).map((r) => r.url),
  repositoryGroups: form.repositoryGroups || [],
  keywords: form.keywords,
  searchKeywords: form.searchKeywords,
  widgets: Object.keys(form.widgets).filter(
    (key: string) => form.widgets[key].enabled,
  ),
});

export const buildForm = (
  result: InsightsProjectModel,
  repositories: {
    url: string;
    label: string;
    enabled: boolean;
    platforms: string[];
  }[],
): InsightsProjectAddFormModel => ({
  ...result,
  organizationId: result.organization.id,
  collectionsIds: result.collections.map((collection: any) => collection.id),
  collections: result.collections,
  keywords: result.keywords || [],
  searchKeywords: result.searchKeywords || [],
  repositoryGroups: result.repositoryGroups || [],
  repositories: repositories || [],
  widgets: Object.fromEntries(
    getDefaultWidgets().map((key) => [
      key,
      {
        enabled: result.widgets?.includes(key) || false,
      },
    ]),
  ),
});

export const buildRepositories = (
  res: Record<string, Array<{ url: string; label: string; enabled: boolean }>>,
) => {
  const urlMap = new Map<
    string,
    {
      url: string;
      label: string;
      enabled: boolean;
      platforms: string[];
    }
  >();

  // Iterate through each platform (git, github, gitlab, gerrit)
  Object.entries(res).forEach(([platform, repos]) => {
    // Process each repository from the platform
    repos.forEach((repo) => {
      if (urlMap.has(repo.url)) {
        // If URL exists, add the platform to its platforms array
        const existing = urlMap.get(repo.url)!;
        existing.platforms.push(platform);
        if (repo.enabled) {
          existing.enabled = true;
        }
      } else {
        // If URL is new, create a new entry
        urlMap.set(repo.url, {
          url: repo.url,
          label: repo.label,
          enabled: repo.enabled,
          platforms: [platform],
        });
      }
    });
  });

  return Array.from(urlMap.values());
};
