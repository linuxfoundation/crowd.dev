import { defineStore, storeToRefs } from 'pinia';
import { computed, ref } from 'vue';
import { LfService } from '@/modules/lf/segments/lf-segments-service';
import { ToastStore } from '@/shared/message/notification';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import { LfRole } from '@/shared/modules/permissions/types/Roles';
import { getAxiosErrorMessage } from '@/shared/helpers/error-message.helper';
import type { AxiosError } from 'axios';
import type {
  ProjectGroup,
  Project,
  SubProject,
  ProjectGroupRequest,
  ProjectRequest,
  CreateProjectRequest,
  SubProjectRequest,
  CreateSubProjectRequest,
} from '@/modules/lf/segments/types/Segments';

interface Pagination {
  pageSize: number;
  currentPage: number;
  total: number;
  count: number;
}

export interface SegmentsState {
  selectedProjectGroup: ProjectGroup | null;
  selectedProjectGroupSubprojects: SubProject[];
  adminProjectGroups: {
    list: ProjectGroup[];
  };
  projectGroups: {
    list: ProjectGroup[];
    loading: boolean;
    paginating: boolean;
    reload?: boolean;
    pagination: Pagination;
  };
  projects: {
    list: Project[];
    parentSlug: string;
    loading: boolean;
    paginating: boolean;
    pagination: Pagination;
  };
}

interface ListResponse<T> {
  rows: T[];
  count: number | string;
}

interface ListProjectGroupsParams {
  search?: string | null;
  offset?: number;
  limit?: number | null;
  reset?: boolean;
  adminOnly?: boolean;
}

interface ListProjectsParams {
  parentSlug?: string | null;
  search?: string | null;
  offset?: number;
  limit?: number | null;
  reset?: boolean;
}

type SegmentId = string | string[] | null;

const initialPagination = (): Pagination => ({
  pageSize: 20,
  currentPage: 1,
  total: 0,
  count: 0,
});

const isAdminOnly = () => {
  const authStore = useAuthStore();
  const { roles } = storeToRefs(authStore);

  return roles.value.includes(LfRole.projectAdmin);
};

export const useLfSegmentsStore = defineStore('lf-segments', () => {
  const selectedProjectGroup = ref<SegmentsState['selectedProjectGroup']>(null);
  const selectedProjectGroupSubprojects = ref<SegmentsState['selectedProjectGroupSubprojects']>([]);
  const adminProjectGroups = ref<SegmentsState['adminProjectGroups']>({
    list: [],
  });
  const projectGroups = ref<SegmentsState['projectGroups']>({
    list: [],
    loading: true,
    paginating: false,
    pagination: initialPagination(),
  });
  const projects = ref<SegmentsState['projects']>({
    list: [],
    parentSlug: '',
    loading: true,
    paginating: false,
    pagination: initialPagination(),
  });

  const projectGroupOffset = computed(() => {
    const currentPage = projectGroups.value.pagination.currentPage || 1;

    return (currentPage - 1) * projectGroups.value.pagination.pageSize;
  });

  const projectOffset = computed(() => {
    const currentPage = projects.value.pagination.currentPage || 1;

    return (currentPage - 1) * projects.value.pagination.pageSize;
  });

  function listProjectGroups({
    search = null,
    offset,
    limit,
    reset = false,
    adminOnly,
  }: ListProjectGroupsParams = {}) {
    if (reset) {
      projectGroups.value.pagination = initialPagination();
    }
    const offsetLoad = offset !== undefined ? offset : projectGroupOffset.value;
    const limitLoad = limit !== undefined ? limit : projectGroups.value.pagination.pageSize;
    projectGroups.value.loading = offsetLoad === 0;
    projectGroups.value.paginating = offsetLoad > 0;

    return LfService.queryProjectGroups({
      limit: limitLoad,
      offset: offsetLoad,
      filter: {
        name: search,
        adminOnly,
      },
    })
      .then((response: ListResponse<ProjectGroup>) => {
        const count = Number(response.count);

        if (offsetLoad === 0 || reset) {
          projectGroups.value.list = response.rows;
        } else {
          projectGroups.value.list = [...projectGroups.value.list, ...response.rows];
        }

        if (!search) {
          projectGroups.value.pagination.total = count;
        }

        projectGroups.value.pagination.count = count;
        return Promise.resolve();
      })
      .catch(() => {
        ToastStore.error('Something went wrong while fetching project groups');
        return Promise.reject();
      })
      .finally(() => {
        projectGroups.value.paginating = false;
        projectGroups.value.loading = false;
      });
  }

  function listAdminProjectGroups() {
    return LfService.queryProjectGroups({
      limit: null,
      offset: 0,
      filter: {
        adminOnly: true,
      },
    })
      .then((response: ListResponse<ProjectGroup>) => {
        adminProjectGroups.value.list = response.rows;
        return Promise.resolve();
      })
      .catch(() => Promise.reject());
  }

  function findProjectGroup(id: SegmentId) {
    return LfService.findSegment(id)
      .then((projectGroup: ProjectGroup) => Promise.resolve(projectGroup))
      .catch(() => {
        ToastStore.error('Something went wrong while getting the project group');
        return undefined;
      });
  }

  function updateProjectGroupList(id: SegmentId, data: ProjectGroupRequest) {
    projectGroups.value.list = projectGroups.value.list.map((p) => {
      if (p.id === id) {
        // shortcut: the group becomes `data` alone, as on main (it drops id). revisit: when edit should keep the entry's id
        return { ...data } as unknown as ProjectGroup;
      }
      return p;
    });
    projectGroups.value.reload = true;
  }

  function createProjectGroup(data: ProjectGroupRequest) {
    return LfService.createProjectGroup(data)
      .then(() => {
        ToastStore.success('Project Group created successfully');

        listProjectGroups({
          adminOnly: isAdminOnly(),
        });
      })
      .catch(() => {
        ToastStore.error('Something went wrong while creating the project group');
      })
      .finally(() => Promise.resolve());
  }

  function updateProjectGroup(id: SegmentId, data: ProjectGroupRequest) {
    return LfService.updateSegment(id, data)
      .then(() => {
        ToastStore.success('Project Group updated successfully');

        updateProjectGroupList(id, data);
      })
      .catch(() => {
        ToastStore.error('Something went wrong while updating the project group');
      })
      .finally(() => Promise.resolve());
  }

  function searchProjectGroup(
    search: string | null,
    limit?: number | null,
    adminOnly?: boolean,
    page?: number,
  ) {
    projectGroups.value.pagination.currentPage = page !== undefined ? page : 1;
    listProjectGroups({
      search,
      limit,
      offset: page !== undefined ? undefined : 0,
      adminOnly: adminOnly !== undefined ? adminOnly : isAdminOnly(),
    });
  }

  function listProjects({
    parentSlug = null,
    search = null,
    offset,
    limit,
    reset = false,
  }: ListProjectsParams = {}) {
    projects.value.loading = true;

    if (parentSlug) {
      projects.value.parentSlug = parentSlug;
    }

    if (reset) {
      projects.value.pagination = initialPagination();
    }

    const offsetLoad = offset !== undefined ? offset : projectOffset.value;
    const limitLoad = limit !== undefined ? limit : projects.value.pagination.pageSize;
    projects.value.loading = offsetLoad === 0;
    projects.value.paginating = offsetLoad > 0;

    LfService.queryProjects({
      limit: limitLoad,
      offset: offsetLoad,
      filter: {
        name: search,
        parentSlug: projects.value.parentSlug,
      },
    })
      .then((response: ListResponse<Project>) => {
        const count = Number(response.count);

        if (offsetLoad === 0 || reset) {
          projects.value.list = response.rows;
        } else {
          projects.value.list = [...projects.value.list, ...response.rows];
        }

        if (!search) {
          projects.value.pagination.total = count;
        }

        projects.value.pagination.count = count;
      })
      .catch(() => {
        ToastStore.error('Something went wrong while fetching projects');
      })
      .finally(() => {
        projects.value.loading = false;
        projects.value.paginating = false;
      });
  }

  function findProject(id: SegmentId) {
    return LfService.findSegment(id)
      .then((project: Project) => Promise.resolve(project))
      .catch(() => {
        ToastStore.error('Something went wrong while getting the project');
        return undefined;
      });
  }

  function updateProjectList(id: SegmentId, data: Partial<ProjectRequest>) {
    projects.value.list = projects.value.list.map((p) => {
      if (p.id === id) {
        return { ...p, ...data };
      }
      return p;
    });
  }

  function createProject(data: CreateProjectRequest) {
    return LfService.createProject(data)
      .then(() => {
        ToastStore.success('Project created successfully');
        listProjects();
      })
      .catch((err: AxiosError) => {
        ToastStore.error(
          getAxiosErrorMessage(err, 'Something went wrong while creating the project'),
        );
      })
      .finally(() => Promise.resolve());
  }

  function updateProject(id: SegmentId, data: Partial<ProjectRequest>) {
    return LfService.updateSegment(id, data)
      .then(() => {
        ToastStore.success('Project updated successfully');
        updateProjectList(id, data);
      })
      .catch((err: AxiosError) => {
        ToastStore.error(
          getAxiosErrorMessage(err, 'Something went wrong while updating the project'),
        );
      })
      .finally(() => Promise.resolve());
  }

  function searchProject(search: string, page: number | null = null) {
    projects.value.pagination.currentPage = page !== null ? page : 1;
    listProjects({ search, offset: page !== null ? undefined : 0 });
  }

  function findSubProject(id: SegmentId) {
    return LfService.findSegment(id)
      .then((project: Project) => Promise.resolve(project))
      .catch(() => {
        ToastStore.error('Something went wrong while getting the sub-project');
        return undefined;
      });
  }

  function createSubProject(data: CreateSubProjectRequest) {
    return LfService.createSubProject(data)
      .then(() => {
        ToastStore.success('Sub-project created successfully');
        listProjects();
      })
      .catch((err: AxiosError) => {
        ToastStore.error(
          getAxiosErrorMessage(err, 'Something went wrong while creating the sub-project'),
        );
      })
      .finally(() => Promise.resolve());
  }

  function updateSubProject(id: SegmentId, data: SubProjectRequest) {
    return LfService.updateSegment(id, data)
      .then(() => {
        ToastStore.success('Sub-project updated successfully');
        listProjects();
      })
      .catch((err: AxiosError) => {
        ToastStore.error(
          getAxiosErrorMessage(err, 'Something went wrong while updating the sub-project'),
        );
      })
      .finally(() => Promise.resolve());
  }

  function updateProjectGroupsPageSize(pageSize: number) {
    projectGroups.value.pagination.pageSize = pageSize;

    listProjectGroups();
  }

  function updateProjectsPageSize(pageSize: number) {
    projects.value.pagination.pageSize = pageSize;

    listProjects();
  }

  async function fetchSubprojectsForProjectGroup(grandparentSlug?: string | null) {
    if (!grandparentSlug) {
      selectedProjectGroupSubprojects.value = [];
      return;
    }

    selectedProjectGroupSubprojects.value = [];

    try {
      const response: ListResponse<SubProject> = await LfService.querySubprojectsLite({
        limit: null,
        offset: 0,
        filter: { grandparentSlug },
      });
      if (selectedProjectGroup.value?.slug === grandparentSlug) {
        selectedProjectGroupSubprojects.value = response.rows || [];
      }
    } catch (e) {
      if (selectedProjectGroup.value?.slug === grandparentSlug) {
        selectedProjectGroupSubprojects.value = [];
      }
    }
  }

  async function updateSelectedProjectGroup(
    projectGroupOrId: string | Pick<ProjectGroup, 'id'> | null | undefined,
    sendToDashboard = true,
  ) {
    if (!projectGroupOrId) {
      selectedProjectGroup.value = null;
      selectedProjectGroupSubprojects.value = [];
      return;
    }

    const id = typeof projectGroupOrId === 'string' ? projectGroupOrId : projectGroupOrId.id;
    if (!id) {
      selectedProjectGroup.value = null;
      selectedProjectGroupSubprojects.value = [];
      return;
    }

    try {
      let projectGroup = projectGroups.value.list.find((p) => p.id === id) || null;
      if (!projectGroup) {
        await listProjectGroups({ limit: null, reset: true });
        projectGroup = projectGroups.value.list.find((p) => p.id === id) || null;
      }

      if (!projectGroup?.id) {
        selectedProjectGroup.value = null;
        selectedProjectGroupSubprojects.value = [];
        ToastStore.error('Project group could not be loaded');
        return;
      }

      selectedProjectGroup.value = projectGroup;
      await fetchSubprojectsForProjectGroup(projectGroup.slug);

      if (sendToDashboard) {
        const { router } = await import('@/router');
        router.push({
          name: 'member',
        });
      }
    } catch (e) {
      selectedProjectGroup.value = null;
      selectedProjectGroupSubprojects.value = [];
      ToastStore.error('Something went wrong while selecting the project group');
    }
  }

  function doChangeProjectGroupCurrentPage(currentPage: number) {
    projectGroups.value.pagination.currentPage = currentPage;

    listProjectGroups();
  }

  function doChangeProjectCurrentPage(currentPage: number) {
    projects.value.pagination.currentPage = currentPage;

    listProjects();
  }

  return {
    selectedProjectGroup,
    selectedProjectGroupSubprojects,
    adminProjectGroups,
    projectGroups,
    projects,
    projectGroupOffset,
    projectOffset,
    listProjectGroups,
    listAdminProjectGroups,
    findProjectGroup,
    createProjectGroup,
    updateProjectGroup,
    updateProjectGroupList,
    searchProjectGroup,
    listProjects,
    findProject,
    createProject,
    updateProject,
    updateProjectList,
    searchProject,
    findSubProject,
    createSubProject,
    updateSubProject,
    updateProjectGroupsPageSize,
    updateProjectsPageSize,
    fetchSubprojectsForProjectGroup,
    updateSelectedProjectGroup,
    doChangeProjectGroupCurrentPage,
    doChangeProjectCurrentPage,
  };
});
