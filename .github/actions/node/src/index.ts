import * as core from '@actions/core'

import { getInputs } from './inputs'
import { IS_POST } from './state'
import { buildStep, deployStep, pushStep } from './steps'
import { ActionStep } from './types'
/**
 * Runs the action
 */
async function run() {
  const inputs = await getInputs()

  for (const step of inputs.steps) {
    core.info(`Running step: ${step}`)
    switch (step) {
      case ActionStep.BUILD: {
        await buildStep()
        break
      }

      case ActionStep.PUSH: {
        await pushStep()
        break
      }

      case ActionStep.DEPLOY: {
        await deployStep()
        break
      }

      default:
        core.error(`Unknown action step: ${step}!`)
        throw new Error(`Unknown action step: ${step}!`)
    }
  }
}

/**
 * Runs the post action cleanup step
 */
async function post() {}

setImmediate(async () => {
  if (!IS_POST) {
    await run()
  } else {
    await post()
  }
})
