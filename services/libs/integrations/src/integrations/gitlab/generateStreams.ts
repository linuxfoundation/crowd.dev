import { GenerateStreamsHandler } from '../../types'
import { GitLabSettings, GitlabRootStream, GitlabStreamType } from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  const settings = ctx.integration.settings as GitLabSettings
  // Collect all project IDs and paths from user projects and group projects
  const allProjects = [
    ...settings.userProjects.map((project) => ({
      id: project.id.toString(),
      path_with_namespace: project.path_with_namespace,
      enabled: project.enabled,
    })),
    ...Object.values(settings.groupProjects).flatMap((projects) =>
      projects.map((project) => ({
        id: project.id.toString(),
        path_with_namespace: project.path_with_namespace,
        enabled: project.enabled,
      })),
    ),
  ]

  const projects = allProjects.filter((project) => project.enabled)

  ctx.log.info('Generating GitLab streams for projects:', projects)

  await ctx.publishStream<GitlabRootStream>(GitlabStreamType.ROOT, {
    projects,
  })
}

export default handler
