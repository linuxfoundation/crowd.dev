import { ProjectGroup, Project, SubProject } from '@/modules/lf/segments/types/Segments';

export interface SegmentsState {
  selectedProjectGroup: ProjectGroup | null
  selectedProjectGroupSubprojects: SubProject[]
  adminProjectGroups: {
    list: ProjectGroup[]
  }
  projectGroups: {
    list: ProjectGroup[]
    loading: boolean
    paginating: boolean
    pagination: {
      pageSize: number
      currentPage: number
      total: number
      count: number
    }
  }
  projects: {
    list: Project[]
    parentSlug: string
    loading: boolean
    paginating: boolean
    pagination: {
      pageSize: number
      currentPage: number
      total: number
      count: number
    }
  }
}

const state: SegmentsState = {
  selectedProjectGroup: null,
  selectedProjectGroupSubprojects: [],
  adminProjectGroups: {
    list: [],
  },
  projectGroups: {
    list: [],
    loading: true,
    pagination: {
      pageSize: 20,
      currentPage: 1,
      total: 0,
      count: 0,
    },
  },
  projects: {
    list: [],
    parentSlug: '',
    loading: true,
    pagination: {
      pageSize: 20,
      currentPage: 1,
      total: 0,
      count: 0,
    },
  },
};

export default () => state;
