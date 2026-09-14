export interface SubProject {
  id: string;
  name: string;
  status: string;
  integrations?: {
    id: string
    platform: string;
    status: string;
    type?: string;
    [key: string]: string | undefined;
  }[];
}

export interface Project {
  id: string;
  url: string;
  name: string;
  parentName: string;
  grandparentName: string;
  slug: string;
  parentSlug: string;
  grandparentSlug: string;
  status: string;
  description: string;
  sourceId: string;
  sourceParentId: string;
  customActivityTypes: string;
  activityChannels: object;
  createdAt: string;
  updatedAt: string;
  subproject_count: number;
  subprojects: SubProject[];
  isLF: boolean;
}

export interface ProjectGroup {
  id: string;
  url: string;
  name: string;
  parentName: string;
  grandparentName: string;
  slug: string;
  parentSlug: string;
  grandparentSlug: string;
  status: string;
  description: string;
  sourceId: string;
  sourceParentId: string;
  customActivityTypes: object;
  activityChannels: object;
  createdAt: string;
  updatedAt: string;
  projects?: Project[];
  projectCount?: number;
  isLF: boolean;
}

export interface ProjectRequest {
  name: string;
  slug: string;
  sourceId: string;
  status: string;
  isLF: boolean;
  parentSlug: string;
}
