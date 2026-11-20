export enum ActionStep {
  BUILD = 'build',
  PUSH = 'push',
  DEPLOY = 'deploy',
}

export enum CloudEnvironment {
  PRODUCTION = 'production',
  STAGING = 'staging',
  LF_PRODUCTION = 'lf-production',
  LF_ORACLE_PRODUCTION = 'lf-oracle-production',
  LF_ORACLE_STAGING = 'lf-oracle-staging',
  LF_STAGING = 'lf-staging',
}

export interface IBuildInput {
  // image version tag
  tag: string

  // images to build
  images: string[]
}

export interface IPushInput {
  // docker credentials to push image to
  dockerUsername: string
  dockerPassword: string
}

export interface IAwsDeployInput {
  // aws credentials to use when deploying
  eksClusterName: string
  awsRoleArn: string
  awsAccessKeyId: string
  awsSecretAccessKey: string
  awsRegion: string
}

export interface IOracleDeployInput {
  cluster: string
  user: string
  tenant: string
  region: string
  fingerprint: string
  key: string
}

export interface IDeployInput {
  // services to deploy
  services: string[]

  // which cloud environment are we deploying to
  cloudEnvironment: CloudEnvironment

  aws?: IAwsDeployInput
  oracle?: IOracleDeployInput
}

export interface IBuilderDefinition {
  imageName: string
  dockerRepository: string
  services: string[]
  prioritizedServices: string[]
  cronJobs: string[]
}

export type IActionInputs = {
  steps: ActionStep[]
  [ActionStep.BUILD]?: IBuildInput
  [ActionStep.PUSH]?: IPushInput
  [ActionStep.DEPLOY]?: IDeployInput
}
