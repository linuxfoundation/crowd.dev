import axios from 'axios'

export async function fetchAllGitlabGroups(accessToken: string) {
  const groups = []
  let page = 1
  let hasMorePages = true

  while (hasMorePages) {
    const response = await axios.get('https://gitlab.com/api/v4/groups', {
      headers: { Authorization: `Bearer ${accessToken}` },
      params: { page, per_page: 100 },
    })
    groups.push(...response.data)
    hasMorePages = response.headers['x-next-page'] !== ''
    page++
  }

  return groups.map((group) => ({
    id: group.id,
    name: group.name as string,
    path: group.path as string,
    avatarUrl: group.avatar_url as string,
  }))
}

async function fetchProjectsForGroup(accessToken: string, group: any) {
  const projects = []
  let page = 1
  let hasMorePages = true

  while (hasMorePages) {
    const response = await axios.get(`https://gitlab.com/api/v4/groups/${group.id}/projects`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      params: { page, per_page: 100, archived: false },
    })
    projects.push(...response.data)
    hasMorePages = response.headers['x-next-page'] !== ''
    page++
  }

  return projects.map((project) => ({
    groupId: group.id,
    groupName: group.name,
    groupPath: group.path,
    id: project.id,
    name: project.name,
    path_with_namespace: project.path_with_namespace,
    enabled: false,
    forkedFrom: project?.forked_from_project?.web_url || null,
  }))
}

export async function fetchGitlabGroupProjects(accessToken: string, groups: any[]) {
  const CONCURRENCY = 10
  const groupProjects: Record<number, any[]> = {}

  for (let i = 0; i < groups.length; i += CONCURRENCY) {
    const batch = groups.slice(i, i + CONCURRENCY)
    const results = await Promise.all(
      batch.map((group) => fetchProjectsForGroup(accessToken, group)),
    )
    batch.forEach((group, idx) => {
      groupProjects[group.id] = results[idx]
    })
  }

  return groupProjects
}

export async function fetchGitlabUserProjects(accessToken: string, userId: number) {
  const projects = []
  let page = 1
  let hasMorePages = true

  while (hasMorePages) {
    const response = await axios.get(`https://gitlab.com/api/v4/users/${userId}/projects`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      params: { page, per_page: 100, archived: false },
    })
    projects.push(...response.data)
    hasMorePages = response.headers['x-next-page'] !== ''
    page++
  }

  return projects.map((project) => ({
    id: project.id,
    name: project.name,
    path_with_namespace: project.path_with_namespace,
    enabled: false,
    forkedFrom: project?.forked_from_project?.web_url || null,
  }))
}
