import type { SegmentsState } from './state';

export default {
  projectGroupOffset: (state: SegmentsState) => {
    const currentPage = state.projectGroups.pagination.currentPage || 1;

    return (currentPage - 1) * state.projectGroups.pagination.pageSize;
  },
  projectOffset: (state: SegmentsState) => {
    const currentPage = state.projects.pagination.currentPage || 1;

    return (currentPage - 1) * state.projects.pagination.pageSize;
  },
};
