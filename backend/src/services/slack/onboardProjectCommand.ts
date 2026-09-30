import { randomUUID } from 'crypto'

import axios from 'axios'
import { Actions, Bits, Button, Message, Section, SlackMessageDto } from 'slack-block-builder'

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
  setProjectCatalogSourceUrl,
  updateProjectCatalog,
} from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { onboardProject } from '@crowd/project-onboarding'
import { getSlackPermalink, postSlackMessage } from '@crowd/slack'

import { createDailyProjectCatalogCap } from '../../api/public/v1/projectCatalog/dailyRequestCap'
import { createDailyLlmCap } from '../../api/public/v1/projectEvaluation/dailyLlmCap'
import { evaluateProject } from '../../api/public/v1/projectEvaluation/evaluateProject'
import { IServiceOptions } from '../IServiceOptions'
import { FORCE_ONBOARDING_ACTION_ID } from './slackActionIds'
import { getBgQx, reserveDailyProjectOnboardingRequest } from './slackBackground'
import { type SlackBotRequestAlertKind, notifySlackBotRequest } from './slackBotRequestAlert'

const reserveDailyProjectCatalogRequest = createDailyProjectCatalogCap()
const reserveDailyLlmCall = createDailyLlmCap()

export function textMessage(text: string): SlackMessageDto {
  return Message().blocks(Section({ text })).buildToObject()
}

function forceOnboardingPrompt(
  repoUrl: string,
  catalogId: string,
  evaluation: { outcome: string; evaluationReason?: string | null },
): SlackMessageDto {
  const reason = evaluation.evaluationReason ? `\nReason: ${evaluation.evaluationReason}` : ''
  return Message()
    .blocks(
      Section({
        text: `\`${repoUrl}\` evaluation result: *${evaluation.outcome}*${reason}`,
      }),
      Actions().elements(
        Button({ text: 'Force onboarding', actionId: FORCE_ONBOARDING_ACTION_ID, value: catalogId })
          .danger()
          .confirm(
            Bits.ConfirmationDialog({
              title: 'Force onboarding?',
              text: `The evaluation did not approve ${repoUrl}. Onboard it anyway?`,
              confirm: 'Force onboarding',
              deny: 'Cancel',
            }),
          ),
      ),
    )
    .buildToObject()
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

async function recordRequestMessage(
  qx: QueryExecutor,
  {
    catalogId,
    repoUrl,
    channelId,
    actorId,
    log,
  }: {
    catalogId: string
    repoUrl: string
    channelId?: string
    actorId: string
    log: IServiceOptions['log']
  },
): Promise<string | null> {
  try {
    if (!channelId) {
      log.warn({ catalogId, repoUrl }, 'Slack sent no channel id, onboarding request not recorded.')
      return null
    }

    const posted = await postSlackMessage({
      channel: channelId,
      text: `<@${actorId}> requested onboarding of \`${repoUrl}\``,
    })

    if (!posted.ok || !posted.ts) {
      log.warn({ channelId, error: posted.error }, 'Could not post onboarding request to Slack.')
      return null
    }

    const permalink = await getSlackPermalink(channelId, posted.ts)
    if (permalink) {
      await setProjectCatalogSourceUrl(qx, catalogId, permalink)
    }
    return permalink
  } catch (err) {
    log.warn(err, 'Failed to record the Slack message of the onboarding request.')
    return null
  }
}

export async function runOnboardProjectCommand({
  repoUrl: rawRepoUrl,
  options,
  responseUrl,
  actorId,
  channelId,
}: {
  repoUrl: string
  options: IServiceOptions
  responseUrl?: string
  actorId: string
  channelId?: string
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

  const permalink = await recordRequestMessage(qx, {
    catalogId: catalogEntry.id,
    repoUrl,
    channelId,
    actorId,
    log,
  })
  const alert = (kind: SlackBotRequestAlertKind, reason: string) =>
    notifySlackBotRequest(
      kind,
      {
        repoName: catalogEntry.repoName,
        repoUrl,
        sourceUrl: permalink,
      },
      { reason, actorId },
      log,
    )

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
    if (skipped > 0) {
      await alert('skipped', precheckSkipReason)
    }
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
    const rejectionReason = `slack-bot: rejected before evaluation - ${getErrorMessage(err)}`
    const terminated = await markProjectCatalogPreCheckSkipped(qx, catalogEntry.id, rejectionReason)
    await send(
      textMessage(
        terminated > 0
          ? `:no_entry: ${getErrorMessage(err)}`
          : `:no_entry: ${getErrorMessage(err)} (a concurrent request had already moved \`${repoUrl}\` on)`,
      ),
    )
    if (terminated > 0) {
      await alert('skipped', rejectionReason)
    }
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
    await alert('errored', evaluation.evaluationReason ?? 'unknown error')
    return
  }

  if (evaluation.outcome !== 'onboard') {
    await send(forceOnboardingPrompt(repoUrl, catalogEntry.id, evaluation))
    await alert(
      'skipped',
      evaluation.evaluationReason ?? `evaluation result: ${evaluation.outcome}`,
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

  let onboardingResult
  try {
    onboardingResult = await onboardProject({
      id: randomUUID(),
      repoUrl: catalogEntry.repoUrl,
      repoName: catalogEntry.repoName,
      projectSlug: catalogEntry.projectSlug,
    })
  } catch (err) {
    // Only the external call itself reverts the claim — a persistence failure
    // after a real success must not be treated as a failed onboarding.
    await updateProjectCatalog(qx, catalogEntry.id, {
      action: 'error',
      onboardingError: getErrorMessage(err),
      onboardedAt: null,
    })
    await send(textMessage(`\`${repoUrl}\` passed evaluation but onboarding failed unexpectedly.`))
    await alert('errored', getErrorMessage(err))
    return
  }

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
    await alert('errored', onboardingResult.error ?? 'unknown error')
    return
  }

  await updateProjectCatalog(qx, catalogEntry.id, {
    action: 'onboarded',
    onboardingError: null,
  })

  await send(textMessage(`:white_check_mark: \`${repoUrl}\` evaluated and onboarded successfully!`))
}
