import { createAppAuth } from '@octokit/auth-app'
import { graphql } from '@octokit/graphql'
import { request } from '@octokit/request'

import { GITHUB_ISSUE_REPORTER_CONFIG } from '@/conf'
import { Logger } from '@crowd/logging'

interface ICreatedIssue {
  html_url: string
  node_id: string
}

interface IProjectField {
  id?: string
  name?: string
  options?: { id: string; name: string }[]
}

const PROJECT_QUERY = `query($owner: String!, $number: Int!) {
  organization(login: $owner) {
    projectV2(number: $number) {
      id
      fields(first: 50) { nodes { ... on ProjectV2SingleSelectField { id name options { id name } } } }
    }
  }
}`

const ADD_ITEM_MUTATION = `mutation($projectId: ID!, $contentId: ID!) {
  addProjectV2ItemById(input: { projectId: $projectId, contentId: $contentId }) { item { id } }
}`

const SET_FIELD_MUTATION = `mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId, itemId: $itemId, fieldId: $fieldId, value: { singleSelectOptionId: $optionId }
  }) { projectV2Item { id } }
}`

function normalizePrivateKey(rawKey: string): string {
  return rawKey.includes('BEGIN')
    ? rawKey.replace(/\\n/g, '\n')
    : Buffer.from(rawKey, 'base64').toString('utf8')
}

async function getInstallationToken(): Promise<string> {
  const { appId, privateKey, installationId } = GITHUB_ISSUE_REPORTER_CONFIG

  if (!appId || !privateKey || !installationId) {
    throw new Error('GitHub issue reporter credentials are not configured')
  }

  const auth = createAppAuth({ appId, privateKey: normalizePrivateKey(privateKey) })
  const { token } = await auth({ type: 'installation', installationId: Number(installationId) })

  return token
}

export async function createInternalIssue(
  log: Logger,
  title: string,
  body: string,
  labels: string[] = [],
): Promise<ICreatedIssue> {
  const { repo, projectNumber, projectTeam, projectStatus } = GITHUB_ISSUE_REPORTER_CONFIG

  const token = await getInstallationToken()
  const headers = { authorization: `token ${token}` }
  const gql = graphql.defaults({ headers })

  // Resolve the project fields before creating the issue, so a misconfigured project fails
  // without leaving an issue outside the project.
  const [owner] = repo.split('/')
  const { organization } = await gql<{
    organization: { projectV2: { id: string; fields: { nodes: IProjectField[] } } | null }
  }>(PROJECT_QUERY, { owner, number: Number(projectNumber) })
  const project = organization.projectV2
  if (!project) {
    throw new Error(`Project ${owner}#${projectNumber} not found`)
  }
  const fieldValues = (
    [
      ['Team', projectTeam],
      ['Status', projectStatus],
    ] as const
  ).map(([fieldName, optionName]) => {
    const field = project.fields.nodes.find((f) => f.name === fieldName)
    const option = field?.options?.find((o) => o.name === optionName)
    if (!field?.id || !option) {
      throw new Error(`Project field "${fieldName}" has no option "${optionName}"`)
    }
    return { fieldId: field.id, optionId: option.id }
  })

  await ensureLabels(headers, repo, labels)
  const { data: issue } = await request(`POST /repos/${repo}/issues`, {
    headers,
    title,
    body,
    labels,
  })

  // Both mutations are idempotent, so transient failures are retried.
  try {
    await withRetry(async () => {
      const { addProjectV2ItemById } = await gql<{
        addProjectV2ItemById: { item: { id: string } }
      }>(ADD_ITEM_MUTATION, { projectId: project.id, contentId: issue.node_id })

      for (const { fieldId, optionId } of fieldValues) {
        await gql(SET_FIELD_MUTATION, {
          projectId: project.id,
          itemId: addProjectV2ItemById.item.id,
          fieldId,
          optionId,
        })
      }
    })
  } catch (error) {
    log.error(error, `Created ${issue.html_url} but could not add it to the project!`)
  }

  return issue
}

interface IRequestFailure {
  status?: number
  response?: { data?: { errors?: { code?: string }[] } }
}

async function ensureLabels(
  headers: Record<string, string>,
  repo: string,
  labels: string[],
): Promise<void> {
  for (const name of labels) {
    try {
      await request(`GET /repos/${repo}/labels/${encodeURIComponent(name)}`, { headers })
    } catch (error) {
      if ((error as IRequestFailure).status !== 404) throw error
      try {
        await request(`POST /repos/${repo}/labels`, { headers, name, color: 'ededed' })
      } catch (createError) {
        // A concurrent report may have just created it.
        const { status, response } = createError as IRequestFailure
        const exists =
          status === 422 && response?.data?.errors?.some((e) => e.code === 'already_exists')
        if (!exists) throw createError
      }
    }
  }
}

async function withRetry<T>(fn: () => Promise<T>, attempts = 3, delayMs = 500): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn()
    } catch (error) {
      if (attempt >= attempts) throw error
      await new Promise((resolve) => setTimeout(resolve, delayMs * attempt))
    }
  }
}
