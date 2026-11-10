// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import authAxios from '@/shared/axios/auth-axios';
import type { QueryFunction } from '@tanstack/vue-query';
import type { Pagination } from '@/shared/types/Pagination';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type { Actor, AuditLog } from '@/modules/lf/segments/types/AuditLog';
import type {
  CreateProjectRequest,
  CreateSubProjectRequest,
  Project,
  ProjectGroup,
  ProjectGroupRequest,
  ProjectRequest,
  SegmentId,
  SubProject,
  SubProjectRequest,
} from '@/modules/lf/segments/types/Segments';

export interface SegmentsQueryBody {
  limit?: number | null;
  offset?: number;
  filter?: ApiFilter;
  segments?: string[];
}

export interface AuditLogsQueryBody {
  filter?: ApiFilter;
  limit?: number;
  offset?: number;
}

export const findSegment = async <
  T extends ProjectGroup | Project | SubProject = ProjectGroup | Project,
>(
  id: SegmentId,
): Promise<T> => {
  const response = await authAxios.get<T>(`/segment/${id}`, {
    params: {
      segments: [id],
    },
  });

  return response.data;
};

export const listSegmentsByIds = async (
  ids: string[],
): Promise<(ProjectGroup | Project | SubProject)[]> => {
  const response = await authAxios.post('/segment/id', {
    ids,
  });

  return response.data;
};

export const updateSegment = async (
  id: SegmentId,
  data: ProjectGroupRequest | Partial<ProjectRequest> | SubProjectRequest,
): Promise<ProjectGroup | Project> => {
  const response = await authAxios.put(`/segment/${id}`, {
    ...data,
    segments: [id],
  });

  return response.data;
};

export const queryProjectGroups = async (
  body: SegmentsQueryBody,
): Promise<Pagination<ProjectGroup>> => {
  const response = await authAxios.post('/segment/projectGroup/query', {
    ...body,
    excludeSegments: true,
  });

  return response.data;
};

export const queryProjectGroupsQueryFn =
  (
    query: () => Record<string, string | number | object>,
  ): QueryFunction<Pagination<ProjectGroup>> =>
    ({ pageParam = 0 }) =>
      authAxios
        .post<Pagination<ProjectGroup>>('/segment/projectGroup/query', {
          ...query(),
          offset: pageParam,
          excludeSegments: true,
        })
        .then((res) => res.data);

export const createProjectGroup = async (body: ProjectGroupRequest): Promise<ProjectGroup> => {
  const response = await authAxios.post('/segment/projectGroup', {
    ...body,
    excludeSegments: true,
  });

  return response.data;
};

export const queryProjects = async (body: SegmentsQueryBody): Promise<Pagination<Project>> => {
  const response = await authAxios.post('/segment/project/query', {
    ...body,
    ...(body.segments ? { segments: body.segments } : { excludeSegments: true }),
  });

  return response.data;
};

export const createProject = async (body: CreateProjectRequest): Promise<Project> => {
  const response = await authAxios.post('/segment/project', body);

  return response.data;
};

export const querySubprojectsLite = async (
  body: SegmentsQueryBody,
): Promise<Pagination<SubProject>> => {
  const response = await authAxios.post('/segment/subproject/query-lite', {
    ...body,
    excludeSegments: true,
  });

  return response.data;
};

export const createSubProject = async (body: CreateSubProjectRequest): Promise<SubProject> => {
  const response = await authAxios.post('/segment/subproject', body);

  return response.data;
};

export const fetchUsers = async (
  query: string,
  limit: number,
): Promise<(Pick<Actor, 'id' | 'email'> & { label: string })[]> => {
  const params = {
    query,
    limit,
  };

  const response = await authAxios.get('/user/autocomplete', {
    params,
  });

  return response.data;
};

export const getUser = async (id: string): Promise<Pick<Actor, 'id' | 'fullName' | 'email'>> => {
  const response = await authAxios.get(`/user/${id}`);

  return response.data;
};

export const fetchAuditLogs = async (data: AuditLogsQueryBody): Promise<AuditLog[]> => {
  const response = await authAxios.post('/audit-logs/query', data);

  return response.data;
};

export const LfService = {
  findSegment,
  listSegmentsByIds,
  updateSegment,
  queryProjectGroups,
  createProjectGroup,
  queryProjects,
  createProject,
  querySubprojectsLite,
  createSubProject,
  fetchUsers,
  getUser,
  fetchAuditLogs,
};
