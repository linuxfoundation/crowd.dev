import { randomUUID } from 'crypto'

import axios from 'axios'
import { Message, Section, SlackMessageDto } from 'slack-block-builder'

import { canonicalizeGithubRepoUrl, canonicalizeRepoUrl, getErrorMessage } from '@crowd/common'
import {
  claimProjectCatalogForOnboarding,
  claimProjectCatalogForSlackEvaluation,
  computeExclusivelyLfOwners,
  deriveProjectIdentityFromRepoUrl,
  finalizeProjectCatalogEvaluation,
  findGithubOwnersWithLfProjects,
  findGithubOwnersWithNonLfRepos,
  findRepoUrlsInCdp,
  markProjectCatalogPreCheckSkipped,
  resolvePrecheckSkipReason,
  updateProjectCatalog,
} from '@crowd/data-access-layer'
import { WRITE_DB_CONFIG, getDbConnection, pgpQx } from '@crowd/data-access-layer/src/database'
import { onboardProject } from '@crowd/project-onboarding'

import { createDailyProjectCatalogCap } from '../../api/public/v1/projectCatalog/dailyRequestCap'
import { createDailyLlmCap } from '../../api/public/v1/projectEvaluation/dailyLlmCap'
import { evaluateProject } from '../../api/public/v1/projectEvaluation/evaluateProject'
import { createDailyProjectOnboardingCap } from '../../api/public/v1/projectOnboarding/dailyRequestCap'
import { IServiceOptions } from '../IServiceOptions'

const reserveDailyProjectCatalogRequest = createDailyProjectCatalogCap()
const reserveDailyLlmCall = createDailyLlmCap()
const reserveDailyProjectOnboardingRequest = createDailyProjectOnboardingCap()

// optionsBgQx is Sequelize-backed and always issues QueryTypes.SELECT, which the
// API service's replication config routes to the read replica — fatal for our
// INSERT/UPDATE ... RETURNING claims. Pin this detached workflow to the writer instead.
async function getBgQx() {
  const db = await getDbConnection(WRITE_DB_CONFIG())
  return pgpQx(db)
}

export function textMessage(text: string): SlackMessageDto {
  return Message().blocks(Section({ text })).buildToObject()
}

export async function postToResponseUrl(
  responseUrl: string | undefined,
  message: SlackMessageDto,
  log: IServiceOptions['log'],
): Promise<void> {
  if (!responseUrl) {
    log.warn('No Slack response_url available, dropping onboard-project result.')
    return
  }

  try {
    await axios.post(responseUrl, message)
  } catch (err) {
    log.error(err, 'Failed to post onboard-project result back to Slack.')
  }
}

export async function runOnboardProjectCommand({
  repoUrl: rawRepoUrl,
  options,
  responseUrl,
  actorId,
}: {
  repoUrl: string
  options: IServiceOptions
  responseUrl?: string
  actorId: string
}): Promise<void> {
  const { log } = options
  // Runs detached after the Slack ack (fire-and-forget) — never bind to a
  // request-scoped transaction that may already be committed/rolled back.
  const qx = await getBgQx()
  const send = (message: SlackMessageDto) => postToResponseUrl(responseUrl, message, log)

  const repoUrl = canonicalizeGithubRepoUrl(rawRepoUrl)
  if (!repoUrl) {
    await send(textMessage(`Invalid GitHub repo URL: \`${rawRepoUrl}\``))
    return
  }

  const identity = deriveProjectIdentityFromRepoUrl(repoUrl)
  if (!identity) {
    await send(textMessage(`Unable to derive project identity from repo URL: \`${repoUrl}\``))
    return
  }

  try {
    reserveDailyProjectCatalogRequest(actorId)
  } catch (err) {
    await send(textMessage(`:no_entry: ${getErrorMessage(err)}`))
    return
  }

  const catalogEntry = await claimProjectCatalogForSlackEvaluation(qx, {
    ...identity,
    repoUrl,
    provenance: 'slack-bot',
  })

  if (!catalogEntry) {
    await send(
      textMessage(
        `\`${repoUrl}\` is already onboarded, or is currently being onboarded/evaluated.`,
      ),
    )
    return
  }

  const canonical = canonicalizeRepoUrl(repoUrl)
  const owners = canonical?.isGithub ? [canonical.owner] : []
  const [reposInCdp, lfOwners, nonLfOwners] = await Promise.all([
    findRepoUrlsInCdp(qx, [repoUrl]),
    findGithubOwnersWithLfProjects(qx, owners),
    findGithubOwnersWithNonLfRepos(qx, owners),
  ])
  const precheckSkipReason = resolvePrecheckSkipReason(canonical, {
    reposInCdp,
    exclusivelyLfOwners: computeExclusivelyLfOwners(lfOwners, nonLfOwners),
  })

  if (precheckSkipReason) {
    const skipped = await markProjectCatalogPreCheckSkipped(qx, catalogEntry.id, precheckSkipReason)
    await send(
      textMessage(
        skipped > 0
          ? `\`${repoUrl}\` was skipped: ${precheckSkipReason}`
          : `\`${repoUrl}\` was skipped (${precheckSkipReason}), but a concurrent request had already moved it on — not applying it.`,
      ),
    )
    return
  }

  let evaluation
  try {
    evaluation = await evaluateProject(
      {
        id: catalogEntry.id,
        repoUrl: catalogEntry.repoUrl,
        repoName: catalogEntry.repoName,
        projectSlug: catalogEntry.projectSlug,
        lfCriticalityScore: catalogEntry.lfCriticalityScore,
        source: catalogEntry.source,
      },
      qx,
      {
        accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
        secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
      },
      log,
      () => reserveDailyLlmCall(actorId),
    )
  } catch (err) {
    // Terminate the row here, otherwise the nightly worker re-evaluates it later,
    // bypassing this actor's daily LLM cap.
    const terminated = await markProjectCatalogPreCheckSkipped(
      qx,
      catalogEntry.id,
      `slack-bot: rejected before evaluation - ${getErrorMessage(err)}`,
    )
    await send(
      textMessage(
        terminated > 0
          ? `:no_entry: ${getErrorMessage(err)}`
          : `:no_entry: ${getErrorMessage(err)} (a concurrent request had already moved \`${repoUrl}\` on)`,
      ),
    )
    return
  }

  const finalized = await finalizeProjectCatalogEvaluation(qx, catalogEntry.id, {
    action: evaluation.outcome,
    evaluationResult: evaluation.evaluationResult,
    evaluationReason: evaluation.evaluationReason,
  })

  if (!finalized) {
    await send(
      textMessage(
        `Evaluation for \`${repoUrl}\` finished as *${evaluation.outcome}*, but the catalog entry had already moved on — not applying it. Check \`/crowd print-tenant\` or re-run the command if this looks wrong.`,
      ),
    )
    return
  }

  if (evaluation.evaluationResult === 'error') {
    await send(
      textMessage(
        `:no_entry: Evaluation for \`${repoUrl}\` failed: ${evaluation.evaluationReason ?? 'unknown error'}`,
      ),
    )
    return
  }

  if (evaluation.outcome !== 'onboard') {
    await send(
      textMessage(
        `\`${repoUrl}\` evaluation result: *${evaluation.outcome}*${
          evaluation.evaluationReason ? `\nReason: ${evaluation.evaluationReason}` : ''
        }\n_Forcing onboarding on a negative evaluation isn't available yet (CM-1805)._`,
      ),
    )
    return
  }

  // Stamps onboardedAt so the nightly worker's onboardedAt-truthy check treats
  // this row as already handled, closing the race without touching that worker.
  const claimed = await claimProjectCatalogForOnboarding(qx, catalogEntry.id)
  if (!claimed) {
    await send(textMessage(`\`${repoUrl}\` was just onboarded by the automatic onboarding job.`))
    return
  }

  try {
    reserveDailyProjectOnboardingRequest(actorId)
  } catch (err) {
    // Release the claim back to 'onboard' rather than 'error' — the row already
    // passed evaluation, so the nightly worker should pick it up, not re-evaluate it.
    await updateProjectCatalog(qx, catalogEntry.id, {
      action: 'onboard',
      onboardedAt: null,
      onboardingError: null,
    })
    await send(
      textMessage(
        `\`${repoUrl}\` passed evaluation, but onboarding is currently rate-limited: ${getErrorMessage(err)}. It will be picked up by the automatic onboarding job.`,
      ),
    )
    return
  }

  try {
    const onboardingResult = await onboardProject({
      id: randomUUID(),
      repoUrl: catalogEntry.repoUrl,
      repoName: catalogEntry.repoName,
      projectSlug: catalogEntry.projectSlug,
    })

    if (onboardingResult.outcome === 'error') {
      await updateProjectCatalog(qx, catalogEntry.id, {
        action: 'error',
        onboardingError: onboardingResult.error ?? 'unknown error',
        onboardedAt: null,
      })
      await send(
        textMessage(
          `\`${repoUrl}\` passed evaluation but onboarding failed: ${onboardingResult.error ?? 'unknown error'}`,
        ),
      )
      return
    }

    await updateProjectCatalog(qx, catalogEntry.id, {
      action: 'onboarded',
      onboardingError: null,
    })

    await send(
      textMessage(`:white_check_mark: \`${repoUrl}\` evaluated and onboarded successfully!`),
    )
  } catch (err) {
    // Revert the claim on any unexpected throw too, otherwise the row is stuck at
    // action='onboard' with onboardedAt set — unretryable and invisible to the nightly worker.
    await updateProjectCatalog(qx, catalogEntry.id, {
      action: 'error',
      onboardingError: getErrorMessage(err),
      onboardedAt: null,
    })
    await send(textMessage(`\`${repoUrl}\` passed evaluation but onboarding failed unexpectedly.`))
  }
}
