import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

export const getSegmentsFromProjectGroup = (
  projectGroup,
  options,
  isGetMethod,
) => {
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

  if (!projectGroup.projects?.length) {
    return [projectGroup.id];
  }

  return projectGroup.projects.reduce((acc, project) => {
    (project.subprojects || []).forEach((subproject) => {
      acc.push(subproject.id);
    });

    return acc;
  }, []);
};

export const getProjectGroupsThroughSegments = (segments) => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { projectGroups } = storeToRefs(lsSegmentsStore);

  return projectGroups.value.list
    .filter((p) => segments?.includes(p.id))
    .map((p) => ({
      id: p.id,
      name: p.name,
    }));
};

export const getSegmentName = (segmentId) => {
  const lsSegmentsStore = useLfSegmentsStore();
  const { projectGroups } = storeToRefs(lsSegmentsStore);

  return projectGroups.value.list.find((p) => p.id === segmentId)?.name ?? '';
};
