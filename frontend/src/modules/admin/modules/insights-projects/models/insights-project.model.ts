import type { CollectionModel } from '../../collections/models/collection.model';

export interface InsightsProjectModel {
  id: string;
  slug: string;
  segmentId: string;
  segment: {
    id: string;
    name: string;
    logo: string;
  };
  name: string;
  description: string;
  logoUrl: string;
  collections: CollectionModel[];
  organization: {
    id: string;
    displayName: string;
    logo: string;
  };
  repositoryGroups?: {
    id: string;
    name: string;
    repositories: string[];
  }[];
  website: string;
  github: string;
  twitter: string;
  linkedin: string;
  starred: boolean;
  enabled: boolean;
  repositories: string[];
  widgets: string[];
  keywords: string[];
  searchKeywords: string[];
}

export interface InsightsProjectRequest {
  segmentId?: string;
  name?: string;
  slug?: string;
  description?: string;
  logoUrl?: string;
  collections?: string[]; // assuming collectionsIds is a string array
  organizationId?: string;
  website?: string;
  github?: string;
  twitter?: string;
  linkedin?: string;
  repositories?: string[];
  repositoryGroups?: {
    name: string;
    repositories: string[];
  }[];
  keywords?: string[];
  searchKeywords?: string[];
  widgets?: string[]; // enabled widget keys
  enabled?: boolean;
}

export interface InsightsProjectDetailsResponse {
  description?: string;
  github?: string;
  logoUrl?: string;
  name?: string;
  topics?: string[];
  twitter?: string;
  website?: string;
}
