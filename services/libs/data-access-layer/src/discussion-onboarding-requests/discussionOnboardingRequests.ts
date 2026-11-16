import { QueryExecutor } from '../queryExecutor'
import { prepareSelectColumns } from '../utils'
import { IDbDiscussionOnboardingRequest, IDbDiscussionOnboardingRequestCreate } from './types'

const COLUMNS = [
  'sourceUrl',
  'discussionId',
  'commentId',
  'notes',
  'doneNotifiedAt',
  'createdAt',
  'updatedAt',
]

export async function upsertDiscussionOnboardingRequest(
  qx: QueryExecutor,
  data: IDbDiscussionOnboardingRequestCreate,
): Promise<IDbDiscussionOnboardingRequest> {
  return qx.selectOne(
    `
    INSERT INTO "discussionOnboardingRequests" ("sourceUrl", "discussionId", "commentId", "notes")
    VALUES ($(sourceUrl), $(discussionId), $(commentId), $(notes)::jsonb)
    ON CONFLICT ("sourceUrl") DO UPDATE SET
      "discussionId" = EXCLUDED."discussionId",
      "commentId" = EXCLUDED."commentId",
      "notes" = EXCLUDED."notes",
      "doneNotifiedAt" = NULL,
      "updatedAt" = NOW()
    RETURNING ${prepareSelectColumns(COLUMNS)}
    `,
    { ...data, notes: JSON.stringify(data.notes ?? []) },
  )
}

export async function setDiscussionOnboardingRequestNotes(
  qx: QueryExecutor,
  sourceUrl: string,
  notes: string[],
): Promise<IDbDiscussionOnboardingRequest> {
  return qx.selectOne(
    `
    UPDATE "discussionOnboardingRequests"
    SET "notes" = $(notes)::jsonb, "updatedAt" = NOW()
    WHERE "sourceUrl" = $(sourceUrl)
    RETURNING ${prepareSelectColumns(COLUMNS)}
    `,
    { sourceUrl, notes: JSON.stringify(notes) },
  )
}

// Replaces a comment that was deleted on GitHub without reopening the done notice.
export async function replaceDiscussionOnboardingRequestComment(
  qx: QueryExecutor,
  sourceUrl: string,
  commentId: string,
): Promise<IDbDiscussionOnboardingRequest> {
  return qx.selectOne(
    `
    UPDATE "discussionOnboardingRequests"
    SET "commentId" = $(commentId), "updatedAt" = NOW()
    WHERE "sourceUrl" = $(sourceUrl)
    RETURNING ${prepareSelectColumns(COLUMNS)}
    `,
    { sourceUrl, commentId },
  )
}

export async function findDiscussionOnboardingRequest(
  qx: QueryExecutor,
  sourceUrl: string,
): Promise<IDbDiscussionOnboardingRequest | null> {
  return qx.selectOneOrNone(
    `
    SELECT ${prepareSelectColumns(COLUMNS)}
    FROM "discussionOnboardingRequests"
    WHERE "sourceUrl" = $(sourceUrl)
    `,
    { sourceUrl },
  )
}

// Held until the transaction ends; serializes the read-then-edit of one discussion's status
// comment across the backend and the two workers so an older snapshot cannot overwrite a newer one.
export async function lockDiscussionOnboardingRequest(
  qx: QueryExecutor,
  sourceUrl: string,
): Promise<void> {
  await qx.result(
    `SELECT pg_advisory_xact_lock(hashtextextended('discussionOnboardingRequest:' || $(sourceUrl)::text, 0))`,
    { sourceUrl },
  )
}

export async function claimDiscussionOnboardingRequestDone(
  qx: QueryExecutor,
  sourceUrl: string,
): Promise<boolean> {
  const updated = await qx.result(
    `
    UPDATE "discussionOnboardingRequests"
    SET "doneNotifiedAt" = NOW(), "updatedAt" = NOW()
    WHERE "sourceUrl" = $(sourceUrl) AND "doneNotifiedAt" IS NULL
    `,
    { sourceUrl },
  )
  return updated > 0
}
