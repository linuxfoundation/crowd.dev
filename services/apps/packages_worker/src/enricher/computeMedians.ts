const MS_PER_HOUR = 1000 * 60 * 60

function median(values: number[]): number | null {
  if (values.length === 0) return null
  const sorted = [...values].sort((a, b) => a - b)
  const middleIndex = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0
    ? (sorted[middleIndex - 1] + sorted[middleIndex]) / 2
    : sorted[middleIndex]
}

function hoursBetween(startDate: string, endDate: string): number {
  return (new Date(endDate).getTime() - new Date(startDate).getTime()) / MS_PER_HOUR
}

function toIntHours(hours: number | null): number | null {
  return hours != null ? Math.round(hours) : null
}

// Shapes mirror exactly what GitHub GraphQL returns for comments/reviews nodes
export interface ResponseNode {
  createdAt: string
  author: { login: string } | null
}

export interface PrNode {
  createdAt: string
  mergedAt: string | null
  author: { login: string } | null
  comments: { nodes: ResponseNode[] }
  reviews: { nodes: ResponseNode[] }
}

export interface IssueNode {
  createdAt: string
  closedAt: string | null
  author: { login: string } | null
  comments: { nodes: ResponseNode[] }
}

function firstNonAuthorResponseHours(
  itemCreatedAt: string,
  authorLogin: string | null,
  responses: ResponseNode[],
): number | null {
  const firstResponse = responses.find(
    (response) => response.author?.login && response.author.login !== authorLogin,
  )
  return firstResponse ? hoursBetween(itemCreatedAt, firstResponse.createdAt) : null
}

export function computePrMedians(prs: PrNode[]): {
  medianTimeToMergeHours: number | null
  medianTimeToFirstResponseHours: number | null
} {
  const mergeHours: number[] = []
  const firstResponseHours: number[] = []

  for (const pr of prs) {
    if (pr.mergedAt != null) {
      mergeHours.push(hoursBetween(pr.createdAt, pr.mergedAt))
    }

    const allResponses = [...pr.comments.nodes, ...pr.reviews.nodes].sort(
      (left, right) => new Date(left.createdAt).getTime() - new Date(right.createdAt).getTime(),
    )
    const responseHours = firstNonAuthorResponseHours(
      pr.createdAt,
      pr.author?.login ?? null,
      allResponses,
    )
    if (responseHours != null) firstResponseHours.push(responseHours)
  }

  return {
    medianTimeToMergeHours: toIntHours(median(mergeHours)),
    medianTimeToFirstResponseHours: toIntHours(median(firstResponseHours)),
  }
}

export function computeIssueMedians(issues: IssueNode[]): {
  medianTimeToCloseHours: number | null
  medianTimeToFirstResponseHours: number | null
} {
  const closeHours: number[] = []
  const firstResponseHours: number[] = []

  for (const issue of issues) {
    if (issue.closedAt != null) {
      closeHours.push(hoursBetween(issue.createdAt, issue.closedAt))
    }

    const responseHours = firstNonAuthorResponseHours(
      issue.createdAt,
      issue.author?.login ?? null,
      issue.comments.nodes,
    )
    if (responseHours != null) firstResponseHours.push(responseHours)
  }

  return {
    medianTimeToCloseHours: toIntHours(median(closeHours)),
    medianTimeToFirstResponseHours: toIntHours(median(firstResponseHours)),
  }
}
