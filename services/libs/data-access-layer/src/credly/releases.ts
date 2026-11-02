import { QueryExecutor } from '../queryExecutor'

export interface ICredlyTemplateReleaseInput {
  badgeTemplateId: string
  floor: string
  top: string
}

export interface ICredlyTemplateReleaseCursor {
  top: string
  since: string | null
  until: string | null
  page: number
}

export interface ICredlyTemplateRelease extends ICredlyTemplateReleaseInput {
  since: string | null
  until: string | null
  page: number
  queuedAt: string
  completedAt: string | null
}

const COLUMNS = `"badgeTemplateId", floor, top, since, until, page, "queuedAt", "completedAt"`

export async function queueCredlyTemplateRelease(
  qx: QueryExecutor,
  release: ICredlyTemplateReleaseInput,
): Promise<void> {
  await qx.result(
    `INSERT INTO integration."credlyTemplateReleases" ("badgeTemplateId", floor, top)
     VALUES ($(badgeTemplateId), $(floor), $(top))
     ON CONFLICT ("badgeTemplateId") DO UPDATE
       SET floor = EXCLUDED.floor, top = EXCLUDED.top,
           since = NULL, until = NULL, page = 1,
           "queuedAt" = now(), "completedAt" = NULL, "updatedAt" = now()`,
    release,
  )
}

export async function nextPendingCredlyTemplateRelease(
  qx: QueryExecutor,
): Promise<ICredlyTemplateRelease | null> {
  return qx.selectOneOrNone(
    `SELECT ${COLUMNS} FROM integration."credlyTemplateReleases"
     WHERE "completedAt" IS NULL
     ORDER BY "queuedAt", "badgeTemplateId"
     LIMIT 1`,
  )
}

// Writes carry the queuedAt they read: a release queued again meanwhile is a new walk, and the
// old walker must get 0 rows instead of putting its cursor on the new bounds.
export async function saveCredlyTemplateReleaseCursor(
  qx: QueryExecutor,
  release: Pick<ICredlyTemplateRelease, 'badgeTemplateId' | 'queuedAt'>,
  cursor: ICredlyTemplateReleaseCursor,
): Promise<number> {
  return qx.result(
    `UPDATE integration."credlyTemplateReleases"
     SET top = $(top), since = $(since), until = $(until), page = $(page), "updatedAt" = now()
     WHERE "badgeTemplateId" = $(badgeTemplateId) AND "queuedAt" = $(queuedAt)
       AND "completedAt" IS NULL`,
    { badgeTemplateId: release.badgeTemplateId, queuedAt: release.queuedAt, ...cursor },
  )
}

export interface IUnreleasedMappedCredlyTemplate {
  badgeTemplateId: string
  badgeCount: number | null
}

// Lightest templates first: they drain fast and don't sit behind the handful of
// high-volume ones in the FIFO queue a single release walker works through.
export async function listMappedCredlyTemplatesWithoutRelease(
  qx: QueryExecutor,
): Promise<IUnreleasedMappedCredlyTemplate[]> {
  return qx.select(
    `SELECT m."badgeTemplateId", m."badgeCount"
     FROM integration."credlyTemplateMappings" m
     LEFT JOIN integration."credlyTemplateReleases" r ON r."badgeTemplateId" = m."badgeTemplateId"
     WHERE m.status = 'mapped' AND r."badgeTemplateId" IS NULL
     ORDER BY m."badgeCount" ASC NULLS FIRST, m."badgeTemplateId"`,
  )
}

export async function completeCredlyTemplateRelease(
  qx: QueryExecutor,
  release: Pick<ICredlyTemplateRelease, 'badgeTemplateId' | 'queuedAt'>,
): Promise<number> {
  return qx.result(
    `UPDATE integration."credlyTemplateReleases"
     SET "completedAt" = now(), since = NULL, until = NULL, page = 1, "updatedAt" = now()
     WHERE "badgeTemplateId" = $(badgeTemplateId) AND "queuedAt" = $(queuedAt)
       AND "completedAt" IS NULL`,
    release,
  )
}
