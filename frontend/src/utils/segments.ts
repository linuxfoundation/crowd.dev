import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import type { ProjectGroup, SubProject } from '@/modules/lf/segments/types/Segments';

export const getSegmentsFromProjectGroup = (
  projectGroup: ProjectGroup | null | undefined,
  options?: { url?: string },
  isGetMethod?: boolean,
): string[] => {
  if (!projectGroup) {
    return [];
  }

  if (
    options?.url?.includes('/member/query')
    || options?.url?.includes('/member/active')
    || options?.url?.includes('/organization/query')
    || isGetMethod
  ) {
    return [projectGroup.id];
  }

  const lsSegmentsStore = useLfSegmentsStore();
  const { selectedProjectGroupSubprojects } = storeToRefs(lsSegmentsStore);

  if (!selectedProjectGroupSubprojects.value.length) {
    return [projectGroup.id];
  }

  return selectedProjectGroupSubprojects.value.map((sp: SubProject): string => sp.id);
};

export const getProjectGroupsThroughSegments = (segments?: string[]): Pick<ProjectGroup, 'id' | 'name'>[] => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { projectGroups } = storeToRefs(lsSegmentsStore);

  return projectGroups.value.list
    .filter((p: ProjectGroup): boolean | undefined => segments?.includes(p.id))
    .map((p: ProjectGroup): Pick<ProjectGroup, 'id' | 'name'> => ({
      id: p.id,
      name: p.name,
    }));
};

export const getSegmentName = (segmentId: string): string => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { projectGroups } = storeToRefs(lsSegmentsStore);

  return projectGroups.value.list.find((p: ProjectGroup): boolean => p.id === segmentId)?.name ?? '';
};
