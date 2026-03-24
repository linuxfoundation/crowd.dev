import { generateUUIDv4 } from '@crowd/common'
import {
  ISecurityInsightsEvaluationAssessment,
  ISecurityInsightsEvaluationSuite,
  ISecurityInsightsEvaluations,
  ISecurityInsightsObsoleteRepo,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export async function findObsoleteReposQx(
  qx: QueryExecutor,
  insightsObsoleteAfterSeconds: number,
  failedRepos: string[],
  limit = 1000,
): Promise<ISecurityInsightsObsoleteRepo[]> {
  const failedReposSubquery =
    failedRepos.length > 0 ? 'and r."url" not in ($(failedRepos:csv))' : ''

  const repos: ISecurityInsightsObsoleteRepo[] = await qx.select(
    `
      select distinct
          ip.id as "insightsProjectId",
          ip.slug as "insightsProjectSlug",
          r.url as "repoUrl"
      from "insightsProjects" ip
      join "repositories" r on r."insightsProjectId" = ip.id
      where r."deletedAt" is null
        and r."excluded" = false
        and r.url like 'https://github.com%'
        and not exists (
            select 1
            from "securityInsightsEvaluationSuites" s
            where s.repo = r.url
            AND EXTRACT(EPOCH FROM (now() - s."updatedAt")) < $(insightsObsoleteAfterSeconds)
        )
        ${failedReposSubquery}
      order by r.url asc
      limit $(limit)
    `,
    {
      insightsObsoleteAfterSeconds,
      limit,
      failedRepos: failedRepos.length > 0 ? failedRepos : undefined,
    },
  )

  return repos || []
}

export async function findEvaluationSuite(
  qx: QueryExecutor,
  repo: string,
  catalogId: string,
): Promise<ISecurityInsightsEvaluationSuite | null> {
  return await qx.selectOneOrNone(
    `
      select *
      from "securityInsightsEvaluationSuites"
      where "repo" = $(repo) and "catalogId" = $(catalogId)
    `,
    {
      repo,
      catalogId,
    },
  )
}

export async function addEvaluationSuite(
  qx: QueryExecutor,
  suite: ISecurityInsightsEvaluationSuite,
): Promise<void> {
  await qx.result(
    `
        insert into "securityInsightsEvaluationSuites"
            ("id", "name", "repo", "insightsProjectId", "insightsProjectSlug", "catalogId", "result", "corruptedState", "createdAt", "updatedAt")
        values 
            ($(id), $(name), $(repo), $(insightsProjectId), $(insightsProjectSlug), $(catalogId), $(result), $(corruptedState), now(), now())
        on conflict ("repo", "catalogId")
            do update
            set "updatedAt"      = EXCLUDED."updatedAt",
                "name"           = EXCLUDED."name",
                "result"         = EXCLUDED."result",
                "corruptedState" = EXCLUDED."corruptedState"
    `,
    {
      id: generateUUIDv4(),
      name: suite.name,
      repo: suite.repo,
      insightsProjectId: suite.insightsProjectId,
      insightsProjectSlug: suite.insightsProjectSlug,
      catalogId: suite.catalogId,
      result: suite.result,
      corruptedState: suite.corruptedState,
    },
  )
}

export async function findSuiteControlEvaluation(
  qx: QueryExecutor,
  repo: string,
  controlId: string,
): Promise<ISecurityInsightsEvaluations | null> {
  return await qx.selectOneOrNone(
    `
      select *
      from "securityInsightsEvaluations"
      where "repo" = $(repo) and "controlId" = $(controlId)
    `,
    {
      repo,
      controlId,
    },
  )
}

export async function addSuiteControlEvaluation(
  qx: QueryExecutor,
  evaluation: ISecurityInsightsEvaluations,
): Promise<void> {
  await qx.result(
    `
        insert into "securityInsightsEvaluations"
            (
                "id", 
                "securityInsightsEvaluationSuiteId",
                "name",
                "repo", 
                "insightsProjectId", 
                "insightsProjectSlug",
                "controlId", 
                "result", 
                "message", 
                "corruptedState", 
                "remediationGuide", 
                "createdAt", 
                "updatedAt"
            )
        values 
            (
                $(id), 
                $(securityInsightsEvaluationSuiteId), 
                $(name),
                $(repo), 
                $(insightsProjectId),
                $(insightsProjectSlug),
                $(controlId), 
                $(result),
                $(message),
                $(corruptedState),
                $(remediationGuide),
                now(), 
                now()
            )
        on conflict ("securityInsightsEvaluationSuiteId", "repo", "controlId")
            do update
            set 
                "updatedAt"        = EXCLUDED."updatedAt",
                "name"             = EXCLUDED."name",
                "result"           = EXCLUDED."result",
                "message"          = EXCLUDED."message",
                "corruptedState"   = EXCLUDED."corruptedState",
                "remediationGuide" = EXCLUDED."remediationGuide"
                
    `,
    {
      id: generateUUIDv4(),
      securityInsightsEvaluationSuiteId: evaluation.securityInsightsEvaluationSuiteId,
      name: evaluation.name,
      repo: evaluation.repo,
      insightsProjectId: evaluation.insightsProjectId,
      insightsProjectSlug: evaluation.insightsProjectSlug,
      controlId: evaluation.controlId,
      result: evaluation.result,
      message: evaluation.message,
      corruptedState: evaluation.corruptedState,
      remediationGuide: evaluation.remediationGuide,
    },
  )
}

export async function addControlEvaluationAssessment(
  qx: QueryExecutor,
  assessment: ISecurityInsightsEvaluationAssessment,
): Promise<void> {
  await qx.result(
    `
        insert into "securityInsightsEvaluationAssessments"
            (
                "id", 
                "securityInsightsEvaluationId",
                "repo", 
                "insightsProjectId",
                "insightsProjectSlug",
                "requirementId", 
                "applicability", 
                "description", 
                "result", 
                "message", 
                "steps", 
                "stepsExecuted",
                "runDuration", 
                "recommendation",
                "start",
                "end",
                "value",
                "changes",
                "createdAt", 
                "updatedAt"
            )
        values 
            (
                $(id), 
                $(securityInsightsEvaluationId), 
                $(repo), 
                $(insightsProjectId),
                $(insightsProjectSlug),
                $(requirementId), 
                $(applicability),
                $(description),
                $(result),
                $(message),
                $(steps),
                $(stepsExecuted),
                $(runDuration),
                $(recommendation),
                $(start),
                $(end),
                $(value),
                $(changes),
                now(), 
                now()
            )
        on conflict ("securityInsightsEvaluationId", "repo", "requirementId")
            do update
            set "updatedAt"      = EXCLUDED."updatedAt",
                "applicability"  = EXCLUDED."applicability",
                "description"    = EXCLUDED."description",
                "result"         = EXCLUDED."result",
                "message"        = EXCLUDED."message",
                "steps"          = EXCLUDED."steps",
                "stepsExecuted"  = EXCLUDED."stepsExecuted",
                "runDuration"    = EXCLUDED."runDuration",
                "recommendation" = EXCLUDED."recommendation",
                "start"          = EXCLUDED."start",
                "end"            = EXCLUDED."end",
                "value"          = EXCLUDED."value",
                "changes"        = EXCLUDED."changes"
                
    `,
    {
      id: generateUUIDv4(),
      securityInsightsEvaluationId: assessment.securityInsightsEvaluationId,
      repo: assessment.repo,
      insightsProjectId: assessment.insightsProjectId,
      insightsProjectSlug: assessment.insightsProjectSlug,
      requirementId: assessment.requirementId,
      applicability: assessment.applicability,
      description: assessment.description,
      result: assessment.result,
      message: assessment.message,
      steps: assessment.steps,
      stepsExecuted: assessment.stepsExecuted,
      runDuration: assessment.runDuration,
      recommendation: assessment.recommendation,
      start: assessment.start,
      end: assessment.end,
      value: assessment.value ? JSON.stringify(assessment.value) : null,
      changes: assessment.changes ? JSON.stringify(assessment.changes) : null,
    },
  )
}
