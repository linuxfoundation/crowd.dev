import type { Project } from '@/modules/lf/segments/types/Segments';

export const filterLabel = (value: string[], parentValues: string[], options: Project[]) => {
  let text: string[] = [];

  if (!options?.length) {
    text = ['All'];
  } else {
    options.forEach((project) => {
      const selectedProject = value.includes(project.id);

      if (selectedProject) {
        text.push(`${project.name} (all sub-projects)`);
      } else {
        const selectedSubprojects = project.subprojects.filter(
          (sp) => value.includes(sp.id),
        ).map((sp) => sp.name);

        if (project.subprojects.length === selectedSubprojects.length && parentValues?.includes(project.id)) {
          text.push(`${project.name} (all sub-projects)`);
        } else if (selectedSubprojects.length) {
          text.push(`${selectedSubprojects.join(', ')} (${project.name})`);
        }
      }
    });
  }

  return text.join(', ');
};
