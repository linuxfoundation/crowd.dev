import authAxios from '@/shared/axios/auth-axios';
import type { Pagination } from '@/shared/types/Pagination';
import type { QueryFunction } from '@tanstack/vue-query';
import type { Project, ProjectGroup, ProjectRequest } from './types/Segments';

class SegmentsService {
  queryProjectGroups(
    query: () => Record<string, string | number | object>,
  ): QueryFunction<Pagination<ProjectGroup>> {
    return ({ pageParam = 0 }) => authAxios
      .post<Pagination<ProjectGroup>>('/segment/projectGroup/query', {
        ...query(),
        offset: pageParam,
        excludeSegments: true,
      })
      .then((res) => res.data);
  }

  getSegmentById(id: string) {
    return authAxios
      .get<ProjectGroup | Project>(`/segment/${id}`, {
        params: {
          segments: [id],
        },
      })
      .then((res) => res.data);
  }

  updateSegment(id: string, data: ProjectRequest | ProjectGroup) {
    return authAxios
      .put<ProjectGroup | Project>(`/segment/${id}`, {
        ...data,
        segments: [id],
      })
      .then((res) => res.data);
  }

  createProjectGroup(projectGroup: ProjectGroup) {
    return authAxios
      .post<ProjectGroup>('/segment/projectGroup', {
        ...projectGroup,
        excludeSegments: true,
      })
      .then((res) => res.data);
  }

  createProject(req: {
    project: ProjectRequest;
    segments: string[];

  }) {
    return authAxios
      .post<Project>(
        '/segment/project',
        {
          ...req.project,
          segments: req.segments,
        },
      )
      .then((res) => res.data);
  }
}

export const segmentService = new SegmentsService();
