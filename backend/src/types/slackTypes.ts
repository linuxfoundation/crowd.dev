import { SlackMessageDto } from 'slack-block-builder'

export enum SlackCommand {
  HELP = 'help',
  PRINT_TENANT = 'print-tenant',
  SET_TENANT_PLAN = 'set-tenant-plan',
  ONBOARD_PROJECT = 'onboard-project',
}

export enum SlackCommandParameterType {
  STRING = 'string',
  NUMBER = 'number',
  BOOLEAN = 'boolean',
  UUID = 'uuid',
  DATE = 'date',
}

export interface SlackCommandParameter {
  name: string
  short?: string
  required: boolean
  description: string
  type: SlackCommandParameterType
  default?: any
  allowedValues?: any[]
}

export interface SlackCommandExecutionContext {
  // Slack's slash-command webhook for posting follow-up messages after the initial
  // ack — only present when Slack included one on the originating request.
  responseUrl?: string
  userId?: string
  channelId?: string
}

export interface SlackCommandDefinition {
  command: SlackCommand
  shortVersion?: string
  description: string
  parameters?: SlackCommandParameter[]
  executor: (params: any, context: SlackCommandExecutionContext) => Promise<SlackMessageDto>
}

export interface SlackParameterParseResult {
  params?: any
  error?: SlackMessageDto
}
