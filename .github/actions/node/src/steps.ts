import { getInputs } from './inputs'
import { ActionStep, CloudEnvironment, IBuilderDefinition } from './types'
import * as core from '@actions/core'
import * as exec from '@actions/exec'
import { getBuilderDefinitions } from './utils'
import fs from 'fs'
import os from 'os'
import path from 'path'

const imageTagMap = new Map<string, string>()

export const buildStep = async (): Promise<void> => {
  const inputs = await getInputs()

  if (inputs[ActionStep.BUILD] === undefined) {
    core.error('No build inputs provided!')
    throw new Error('No build inputs provided!')
  }

  const { images, tag } = inputs[ActionStep.BUILD]
  if (images.length === 0) {
    core.error('No images provided!')
    throw new Error('No images provided!')
  }

  if (!tag) {
    core.error('No tag provided!')
    throw new Error('No tag provided!')
  }

  const timestamp = Math.floor(Date.now() / 1000)

  const actualTag = `${tag}.${timestamp}`

  const alreadyBuilt: string[] = []

  for (const image of images) {
    if (alreadyBuilt.includes(image)) {
      core.info(`Skipping already built image: ${image}:${actualTag}`)
      continue
    }

    core.info(`Building image: ${image}:${actualTag}`)
    const exitCode = await exec.exec('bash', ['cli', 'build', image, actualTag], {
      cwd: './scripts',
    })

    if (exitCode !== 0) {
      core.error(`Failed to build image: ${image}:${actualTag}`)
    } else {
      alreadyBuilt.push(image)
      imageTagMap.set(image, actualTag)
    }
  }
}

export const pushStep = async (): Promise<void> => {
  const inputs = await getInputs()
  const images = inputs[ActionStep.BUILD]?.images ?? []
  const pushInput = inputs[ActionStep.PUSH]
  if (!pushInput) {
    core.error('No push inputs provided!')
    throw new Error('No push inputs provided!')
  }

  if (images.length === 0) {
    core.error('No images provided!')
    throw new Error('No images provided!')
  }

  // do a docker login
  const exitCode = await exec.exec('docker', [
    'login',
    'sjc.ocir.io',
    '--username',
    pushInput.dockerUsername,
    '--password',
    pushInput.dockerPassword,
  ])

  if (exitCode !== 0) {
    core.error('Failed to login to docker!')
    throw new Error('Failed to login to docker!')
  }

  // now push the images
  const alreadyPushed: string[] = []
  for (const image of images) {
    if (alreadyPushed.includes(image)) {
      core.info(`Skipping already pushed image: ${image}`)
      continue
    }

    if (!imageTagMap.has(image)) {
      core.warning(`No tag found for image: ${image} - image wasn't built successfully!`)
      continue
    }

    const tag = imageTagMap.get(image)

    core.info(`Pushing image: ${image}:${tag}!`)

    const exitCode = await exec.exec('bash', ['cli', 'push', image, tag], {
      cwd: './scripts',
    })

    if (exitCode !== 0) {
      core.error(`Failed to push image: ${image}:${tag}`)
      imageTagMap.delete(image)
    } else {
      alreadyPushed.push(image)
    }
  }
}

export const deployStep = async (): Promise<void> => {
  const inputs = await getInputs()

  const deployInput = inputs[ActionStep.DEPLOY]
  if (!deployInput) {
    core.error('No deploy inputs provided!')
    throw new Error('No deploy inputs provided!')
  }

  if (deployInput.services.length === 0) {
    core.warning('No services specified for deploy!')
    return
  }

  // check if any images failed to build
  const builderDefinitions = await getBuilderDefinitions()

  const servicesToDeploy: { service: string; tag: string; builderDef: IBuilderDefinition }[] = []
  for (const service of deployInput.services) {
    const builderDef = builderDefinitions.find((b) => b.services.includes(service))

    if (!builderDef) {
      core.error(`No builder definition found for service: ${service}`)
      throw new Error(`No builder definition found for service: ${service}`)
    }

    if (!imageTagMap.has(builderDef.imageName)) {
      core.error(
        `No tag found for image: ${builderDef.imageName} - image wasn't built successfully!`,
      )
      throw new Error(
        `No tag found for image: ${builderDef.imageName} - image wasn't built successfully!`,
      )
    }

    const tag = imageTagMap.get(builderDef.imageName)

    servicesToDeploy.push({
      service,
      tag,
      builderDef,
    })
  }

  let exitCode: number

  if (deployInput.aws) {
    const env = {
      AWS_ACCESS_KEY_ID: deployInput.aws.awsAccessKeyId,
      AWS_SECRET_ACCESS_KEY: deployInput.aws.awsSecretAccessKey,
      AWS_REGION: deployInput.aws.awsRegion,
    }

    exitCode = await exec.exec(
      'aws',
      [
        'eks',
        'update-kubeconfig',
        '--name',
        deployInput.aws.eksClusterName,
        '--role-arn',
        deployInput.aws.awsRoleArn,
      ],
      {
        env,
      },
    )

    if (exitCode !== 0) {
      core.error('Failed to update kubeconfig!')
      throw new Error('Failed to update kubeconfig!')
    }
  } else if (deployInput.oracle) {
    const homeDir = os.homedir()
    const kubeDir = path.join(homeDir, '.kube')
    const ociDir = path.join(homeDir, '.oci')
    const configPath = path.join(ociDir, 'config')
    const keyPath = path.join(ociDir, 'oci_api_key.pem')

    // prepare oracle config
    let config = `
[DEFAULT]
user=${deployInput.oracle.user}
fingerprint=${deployInput.oracle.fingerprint}
key_file=${keyPath}
tenancy=${deployInput.oracle.tenant}
region=${deployInput.oracle.region}
`

    // create the ~/.oci folder if it doesn't exists
    await fs.mkdirSync(ociDir, { recursive: true })

    // write config to ~/.oci/config
    await fs.writeFileSync(configPath, config, 'utf8')

    // write private key to ~/.oci/oci_api_key.pem
    await fs.writeFileSync(keyPath, deployInput.oracle.key, 'utf8')

    // chmod 600 to key and config
    await fs.chmodSync(configPath, 0o600)
    await fs.chmodSync(keyPath, 0o600)

    // get kubernetes context
    await fs.mkdirSync(kubeDir, { recursive: true })

    exitCode = await exec.exec('oci', [
      'ce',
      'cluster',
      'create-kubeconfig',
      '--cluster-id',
      deployInput.oracle.cluster,
      '--file',
      '~/.kube/config',
      '--region',
      deployInput.oracle.region,
      '--token-version',
      '2.0.0',
      '--kube-endpoint',
      'PUBLIC_ENDPOINT',
      '--config-file',
      configPath,
    ])
    if (exitCode !== 0) {
      core.error('Failed to create kubeconfig!')
      throw new Error('Failed to create kubeconfig')
    }
  } else {
    core.error('No cloud provider specified!')
    throw new Error('No cloud provider specified!')
  }

  let failed = []

  for (const serviceDef of servicesToDeploy) {
    const tag = serviceDef.tag
    const service = serviceDef.service
    const prioritized = serviceDef.builderDef.prioritizedServices.includes(service)
    const servicesToUpdate: string[] = []

    if (prioritized) {
      switch (deployInput.cloudEnvironment) {
        case CloudEnvironment.PRODUCTION: {
          servicesToUpdate.push(
            `${service}-system`,
            `${service}-normal`,
            `${service}-high`,
            `${service}-urgent`,
          )
          break
        }
        case CloudEnvironment.LF_ORACLE_PRODUCTION:
        case CloudEnvironment.LF_PRODUCTION: {
          servicesToUpdate.push(`${service}-system`, `${service}-normal`, `${service}-high`)
          break
        }

        case CloudEnvironment.LF_ORACLE_STAGING:
        case CloudEnvironment.LF_STAGING:
        case CloudEnvironment.STAGING: {
          servicesToUpdate.push(`${service}-normal`)
          break
        }

        default:
          core.error(`Unknown cloud environment: ${deployInput.cloudEnvironment}`)
          throw new Error(`Unknown cloud environment: ${deployInput.cloudEnvironment}`)
      }
    } else {
      servicesToUpdate.push(service)
    }

    core.info(
      `Deploying service: ${service} with image: ${
        serviceDef.builderDef.dockerRepository
      }:${tag} to deployments: ${servicesToUpdate.join(', ')}`,
    )

    for (const toDeploy of servicesToUpdate) {
      exitCode = await exec.exec('kubectl', [
        'set',
        'image',
        `deployments/${toDeploy}-dpl`,
        `${toDeploy}=${serviceDef.builderDef.dockerRepository}:${tag}`,
      ])

      if (exitCode !== 0) {
        core.error(`Failed to deploy service: ${service} to deployment: ${toDeploy}`)
        if (!failed.includes(service)) {
          failed.push(service)
        }
      }
    }
  }

  if (failed.length > 0) {
    core.error(`Failed to deploy services: ${failed.join(', ')}`)
    throw new Error(`Failed to deploy services: ${failed.join(', ')}`)
  }
}
