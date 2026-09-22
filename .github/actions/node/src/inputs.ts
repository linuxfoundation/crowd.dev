import * as core from '@actions/core'

import {
  ActionStep,
  CloudEnvironment,
  IActionInputs,
  IAwsDeployInput,
  IBuildInput,
  IDeployInput,
  IOracleDeployInput,
  IPushInput,
} from './types'
import { getBuilderDefinitions } from './utils'

const getBuildInputs = (): IBuildInput => {
  const tag = core.getInput('tag')

  return {
    tag,
    images: [],
  }
}

const getPushInputs = (): IPushInput => {
  const username = process.env.ORACLE_DOCKER_USERNAME
  if (!username) {
    core.error('No Oracle Docker username found in ORACLE_DOCKER_USERNAME environment variable!')
    throw new Error(
      'No Oracle Docker username found in ORACLE_DOCKER_USERNAME environment variable!',
    )
  }

  const password = process.env.ORACLE_DOCKER_PASSWORD
  if (!password) {
    core.error('No Oracle Docker password found in ORACLE_DOCKER_PASSWORD environment variable!')
    throw new Error(
      'No Oracle Docker password found in ORACLE_DOCKER_PASSWORD environment variable!',
    )
  }

  return {
    dockerUsername: username,
    dockerPassword: password,
  }
}

const getDeployIUputs = (): IDeployInput => {
  const services = getInputList('services')

  const cloudEnvironment = process.env['CLOUD_ENV'] as CloudEnvironment
  if (!cloudEnvironment) {
    core.error('No CLOUD_ENV environment variable found!')
    throw new Error('No CLOUD_ENV environment variable found!')
  }

  let aws: IAwsDeployInput | undefined
  let oracle: IOracleDeployInput | undefined

  if (
    cloudEnvironment === CloudEnvironment.LF_ORACLE_PRODUCTION ||
    cloudEnvironment === CloudEnvironment.LF_ORACLE_STAGING
  ) {
    const user = process.env.ORACLE_USER
    if (!user) {
      core.error('No ORACLE_USER environment variable found!')
      throw new Error('No ORACLE_USER environment variable found!')
    }

    const tenant = process.env.ORACLE_TENANT
    if (!tenant) {
      core.error('No ORACLE_TENANT environment variable found!')
      throw new Error('No ORACLE_TENANT environment variable found!')
    }

    const region = process.env.ORACLE_REGION
    if (!region) {
      core.error('No ORACLE_REGION environment variable found!')
      throw new Error('No ORACLE_REGION environment variable found!')
    }

    const fingerprint = process.env.ORACLE_FINGERPRINT
    if (!fingerprint) {
      core.error('No ORACLE_FINGERPRINT environment variable found!')
      throw new Error('No ORACLE_FINGERPRINT environment variable found!')
    }

    const key = process.env.ORACLE_KEY
    if (!key) {
      core.error('No ORACLE_KEY environment variable found!')
      throw new Error('No ORACLE_KEY environment variable found!')
    }

    const cluster = process.env.ORACLE_CLUSTER
    if (!cluster) {
      core.error('No ORACLE_CLUSTER environment variable found!')
      throw new Error('No ORACLE_CLUSTER environment variable found!')
    }

    oracle = {
      user,
      tenant,
      region,
      fingerprint,
      key,
      cluster,
    }
  } else {
    const eksClusterName = process.env.CROWD_CLUSTER
    if (!eksClusterName) {
      core.error('No CROWD_CLUSTER environment variable found!')
      throw new Error('No CROWD_CLUSTER environment variable found!')
    }

    const awsRoleArn = process.env.CROWD_ROLE_ARN
    if (!awsRoleArn) {
      core.error('No CROWD_ROLE_ARN environment variable found!')
      throw new Error('No CROWD_ROLE_ARN environment variable found!')
    }

    const awsAccessKeyId = process.env.AWS_ACCESS_KEY_ID
    if (!awsAccessKeyId) {
      core.error('No AWS_ACCESS_KEY_ID environment variable found!')
      throw new Error('No AWS_ACCESS_KEY_ID environment variable found!')
    }

    const awsSecretAccessKey = process.env.AWS_SECRET_ACCESS_KEY
    if (!awsSecretAccessKey) {
      core.error('No AWS_SECRET_ACCESS_KEY environment variable found!')
      throw new Error('No AWS_SECRET_ACCESS_KEY environment variable found!')
    }

    const awsRegion = process.env.AWS_REGION
    if (!awsRegion) {
      core.error('No AWS_REGION environment variable found!')
      throw new Error('No AWS_REGION environment variable found!')
    }

    aws = {
      eksClusterName,
      awsRoleArn,
      awsAccessKeyId,
      awsSecretAccessKey,
      awsRegion,
    }
  }

  return {
    services,
    cloudEnvironment,
    aws,
    oracle,
  }
}

let inputs: IActionInputs | undefined
export const getInputs = async (): Promise<IActionInputs> => {
  if (inputs !== undefined) {
    return inputs
  }

  const actionSteps = getInputList('steps') as ActionStep[]

  if (actionSteps.length === 0) {
    core.error('No action steps provided!')
    throw new Error('No action steps provided!')
  }

  const results: IActionInputs = {
    steps: actionSteps,
  }

  for (const step of actionSteps) {
    switch (step) {
      case ActionStep.BUILD:
        results[ActionStep.BUILD] = getBuildInputs()
        break
      case ActionStep.PUSH:
        results[ActionStep.PUSH] = getPushInputs()
        break
      case ActionStep.DEPLOY:
        results[ActionStep.DEPLOY] = getDeployIUputs()
        break

      default:
        core.error(`Unknown action step: ${step}!`)
        throw new Error(`Unknown action step: ${step}!`)
    }
  }

  if (results[ActionStep.BUILD] !== undefined) {
    if (results[ActionStep.BUILD].images.length === 0 && results[ActionStep.DEPLOY] !== undefined) {
      // calculate images from services
      const buildDefinitions = await getBuilderDefinitions()

      const images: string[] = []
      for (const service of results[ActionStep.DEPLOY].services) {
        const definition = buildDefinitions.find((d) => d.services.includes(service))
        if (definition === undefined) {
          core.error(`No builder definition found for service: ${service}!`)
          throw new Error(`No builder definition found for service: ${service}!`)
        }

        if (!images.includes(definition.imageName)) {
          images.push(definition.imageName)
        }
      }

      results[ActionStep.BUILD].images = images
    }
  }

  if (results[ActionStep.PUSH] !== undefined && results[ActionStep.BUILD] === undefined) {
    core.error('Push step provided without build step!')
    throw new Error('Push step provided without build step!')
  }

  if (results[ActionStep.DEPLOY] !== undefined && results[ActionStep.PUSH] === undefined) {
    core.error('Deploy step provided without push step!')
    throw new Error('Deploy step provided without push step!')
  }

  inputs = results
  return results
}

const getInputList = (name: string): string[] => {
  const items = core.getInput(name)
  return items
    .split(' ')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
}
