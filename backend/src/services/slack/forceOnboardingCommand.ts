import { randomUUID } from 'crypto'

import { z } from 'zod'

import { getErrorMessage } from '@crowd/common'
import {
  claimProjectCatalogForForcedOnboarding,
  isSlackPermalink,
  updateProjectCatalog,
} from '@crowd/data-access-layer'
import { onboardProject } from '@crowd/project-onboarding'

import { IServiceOptions } from '../IServiceOptions'
import { postToResponseUrl, textMessage } from './onboardProjectCommand'
import { getBgQx, reserveDailyProjectOnboardingRequest } from './slackBackground'
import { notifySlackBotRequest } from './slackBotRequestAlert'

const catalogIdSchema = z.string().uuid()

function replaceOriginal(responseUrl: string, text: string, log: IServiceOptions['log']) {
  const message = { ...textMessage(text), replace_original: true }
  return postToResponseUrl(responseUrl, message, log)
}

export async function runForceOnboardingCommand({
  catalogId: rawCatalogId,
  responseUrl,
  actorId,
  log,
}: {
  catalogId: string
  responseUrl: string
  actorId: string
  log: IServiceOptions['log']
}): Promise<void> {
  const parsedId = catalogIdSchema.safeParse(rawCatalogId)
  if (!parsedId.success) {
    log.warn({ rawCatalogId }, 'Force onboarding click carried an invalid catalog id.')
    await replaceOriginal(responseUrl, ':no_entry: This request is no longer valid.', log)
    return
  }
  const catalogId = parsedId.data

  try {
    reserveDailyProjectOnboardingRequest(actorId)
  } catch (err) {
    await replaceOriginal(responseUrl, `:no_entry: ${getErrorMessage(err)}`, log)
    return
  }

  const qx = await getBgQx()
  const claimed = await claimProjectCatalogForForcedOnboarding(qx, catalogId)
  if (!claimed) {
    await replaceOriginal(
      responseUrl,
      'This repository can no longer be force-onboarded: it was already onboarded, is being onboarded, or was changed since the evaluation.',
      log,
    )
    return
  }

  await replaceOriginal(
    responseUrl,
    `:hourglass_flowing_sand: Onboarding \`${claimed.repoUrl}\`…`,
    log,
  )

  const alertError = (reason: string) =>
    notifySlackBotRequest(
      'errored',
      {
        repoName: claimed.repoName,
        repoUrl: claimed.repoUrl,
        sourceUrl:
          claimed.sourceUrl && isSlackPermalink(claimed.sourceUrl) ? claimed.sourceUrl : null,
      },
      { reason, actorId },
      log,
    )

  let onboardingResult
  try {
    onboardingResult = await onboardProject({
      id: randomUUID(),
      repoUrl: claimed.repoUrl,
      repoName: claimed.repoName,
      projectSlug: claimed.projectSlug,
    })
  } catch (err) {
    await updateProjectCatalog(qx, claimed.id, {
      action: 'error',
      onboardingError: getErrorMessage(err),
      onboardedAt: null,
    })
    await replaceOriginal(
      responseUrl,
      `:x: Forced onboarding of \`${claimed.repoUrl}\` failed unexpectedly.`,
      log,
    )
    await alertError(getErrorMessage(err))
    return
  }

  if (onboardingResult.outcome === 'error') {
    const reason = onboardingResult.error ?? 'unknown error'
    await updateProjectCatalog(qx, claimed.id, {
      action: 'error',
      onboardingError: reason,
      onboardedAt: null,
    })
    await replaceOriginal(
      responseUrl,
      `:x: Forced onboarding of \`${claimed.repoUrl}\` failed: ${reason}`,
      log,
    )
    await alertError(reason)
    return
  }

  await updateProjectCatalog(qx, claimed.id, { action: 'onboarded', onboardingError: null })
  await replaceOriginal(
    responseUrl,
    `:white_check_mark: \`${claimed.repoUrl}\` force-onboarded successfully!`,
    log,
  )
}
