/* eslint-disable no-case-declarations */
import moment from 'moment'
import { Divider, Message, Section, SlackMessageDto } from 'slack-block-builder'

import { validateUUID as uuidValidate } from '@crowd/common'

import { IS_DEV_ENV, IS_PROD_ENV, IS_STAGING_ENV } from '../conf'
import TenantRepository from '../database/repositories/tenantRepository'
import {
  SlackCommand,
  SlackCommandDefinition,
  SlackCommandParameter,
  SlackCommandParameterType,
  SlackParameterParseResult,
} from '../types/slackTypes'
import { IServiceOptions } from './IServiceOptions'

export default class SlackCommandService {
  private readonly commands: SlackCommandDefinition[]

  constructor(private readonly options: IServiceOptions) {
    this.commands = [
      {
        command: SlackCommand.HELP,
        description: 'Prints help message',
        executor: this.printHelp.bind(this),
      },
      {
        command: SlackCommand.PRINT_TENANT,
        shortVersion: 'pt',
        description: 'Prints tenant data as JSON string',
        parameters: [
          {
            name: 'tenantId',
            short: 't',
            required: true,
            description: 'Tenant ID',
            type: SlackCommandParameterType.UUID,
          },
        ],
        executor: this.printTenant.bind(this),
      },
      {
        command: SlackCommand.SET_TENANT_PLAN,
        shortVersion: 'stp',
        description: 'Sets tenant plan to Growth or Essential',
        parameters: [
          {
            name: 'tenantId',
            short: 't',
            required: true,
            description: 'Tenant ID',
            type: SlackCommandParameterType.UUID,
          },
          {
            name: 'plan',
            short: 'p',
            required: true,
            description: 'Plan to set',
            type: SlackCommandParameterType.STRING,
          },
          {
            name: 'trialEndsAt',
            short: 'tea',
            required: false,
            description: 'Trial end date in ISO string format - 2023-03-30',
            type: SlackCommandParameterType.DATE,
          },
        ],
        executor: this.setTenantPlan.bind(this),
      },
    ]
  }

  public async setTenantPlan(params: any): Promise<SlackMessageDto> {
    const plan = params.plan
    const isTrial = params.trialEndsAt !== undefined
    const trialEndsAt = params.trialEndsAt || null
    const tenantId = params.tenantId

    const tenant = await TenantRepository.findById(tenantId, this.options)

    const sections = []

    if (!tenant) {
      sections.push(Section({ text: `*Tenant with ID ${tenantId} not found!*` }))
    } else {
      const result = await TenantRepository.update(
        tenantId,
        {
          plan,
          isTrialPlan: isTrial,
          trialEndsAt,
        },
        this.options,
        true,
      )
      sections.push(
        Section({
          text: `*Tenant ${tenant.name} (${tenantId}) plan changed to ${result.plan}, trial=${
            result.isTrialPlan
          }, trialEndsAt=${result.trialEndsAt ?? '<unset>'}!*`,
        }),
      )
    }

    return Message()
      .blocks(...sections)
      .buildToObject()
  }

  public async printHelp(): Promise<SlackMessageDto> {
    const sections = []
    sections.push(Section({ text: `*Available commands:*` }))
    sections.push(Section({ text: '_Parameters with * are required..._' }))
    sections.push(Divider())

    for (const command of this.commands) {
      let commandString = `*${command.command}${
        command.shortVersion ? `/${command.shortVersion}` : ''
      }*: _${command.description}_`

      if (command.parameters && command.parameters.length > 0) {
        for (const param of command.parameters) {
          commandString += `\n\t${SlackCommandService.paramToSlackString(param)}`
        }
      }

      sections.push(Section({ text: commandString }))
      sections.push(Divider())
    }

    return Message()
      .blocks(...sections)
      .buildToObject()
  }

  public async printTenant(params: any): Promise<SlackMessageDto> {
    const tenantId = params.tenantId
    const tenant = await TenantRepository.findById(tenantId, this.options)

    const sections = []
    if (!tenant) {
      sections.push(Section({ text: `*Tenant with ID ${tenantId} not found!*` }))
    }
    sections.push(
      Section({ text: `*Tenant ${tenant.name} (${tenantId}):*` }),
      Divider(),
      Section({ text: `\`\`\`${JSON.stringify(tenant, null, 2)}\`\`\`` }),
    )

    return Message()
      .blocks(...sections)
      .buildToObject()
  }

  public async processCommand(
    command: string,
    params: string,
    username: string,
    userId: string,
  ): Promise<SlackMessageDto> {
    if (command === '/crowd-test' && !IS_DEV_ENV) {
      this.options.log.error('Received /crowd-test command in non-dev environment! Ignoring!')
      return SlackCommandService.buildSlackMessage(
        Section().text('Received /crowd-test command in non-dev environment! Ignoring!'),
      )
    }

    if (command === '/crowd-staging' && !IS_STAGING_ENV) {
      this.options.log.error(
        'Received /crowd-staging command in non-staging environment! Ignoring!',
      )
      return SlackCommandService.buildSlackMessage(
        Section().text('Received /crowd-staging command in non-staging environment! Ignoring!'),
      )
    }

    if (command === '/crowd' && !IS_PROD_ENV) {
      this.options.log.error('Received /crowd command in non-prod environment! Ignoring!')
      return SlackCommandService.buildSlackMessage(
        Section().text('Received /crowd command in non-prod environment! Ignoring!'),
      )
    }

    this.options.log.info({ command, params, username, userId }, 'Received slack command!')

    const splitParams = params
      .split(' ')
      .map((s) => s.trim())
      .filter((s) => s.length > 0)
    const serviceCommand = splitParams[0]

    const commandDefinition = this.commands.find(
      (c) => c.command === serviceCommand || c.shortVersion === serviceCommand,
    )

    if (!commandDefinition) {
      return SlackCommandService.buildSlackMessage(
        Section().text(`Command not found! Try \`/crowd help\`.`),
      )
    }

    splitParams.shift()

    const parsedParams = SlackCommandService.parseParameters(commandDefinition, splitParams)
    if (parsedParams.error) {
      return parsedParams.error
    }

    return commandDefinition.executor(parsedParams.params)
  }

  private static parseParameters(
    commandDefinition: SlackCommandDefinition,
    params: string[],
  ): SlackParameterParseResult {
    const missingRequiredParameter: SlackCommandParameter[] = []
    const invalidParameters: SlackCommandParameter[] = []

    const result: SlackParameterParseResult = {
      params: {},
    }

    for (const paramDef of commandDefinition.parameters || []) {
      const value = SlackCommandService.findParamValue(paramDef, params)

      if (value) {
        switch (paramDef.type) {
          case SlackCommandParameterType.UUID:
            if (!uuidValidate(value)) {
              invalidParameters.push(paramDef)
            } else {
              result.params[paramDef.name] = value
            }
            break
          case SlackCommandParameterType.BOOLEAN:
            if (value !== 'true' && value !== 'false') {
              invalidParameters.push(paramDef)
            } else {
              result.params[paramDef.name] = value === 'true'
            }
            break
          case SlackCommandParameterType.NUMBER:
            const numericValue = parseInt(value, 10)
            if (Number.isNaN(numericValue)) {
              invalidParameters.push(paramDef)
            } else {
              result.params[paramDef.name] = numericValue
            }
            break
          case SlackCommandParameterType.DATE:
            const dateValue = moment(value)
            if (!dateValue.isValid()) {
              invalidParameters.push(paramDef)
            } else {
              result.params[paramDef.name] = dateValue.toDate()
            }
            break

          case SlackCommandParameterType.STRING:
            result.params[paramDef.name] = value
            break
          default:
            throw new Error(`Unknown parameter type: ${paramDef.type}`)
        }

        if (paramDef.allowedValues && paramDef.allowedValues.indexOf(value) === -1) {
          invalidParameters.push(paramDef)
        }
      } else if (paramDef.required) {
        missingRequiredParameter.push(paramDef)
      }
    }

    const errorBlocks = []
    if (missingRequiredParameter.length > 0) {
      errorBlocks.push(Section({ text: '*Missing required parameters!*' }))
      errorBlocks.push(Divider())
      for (const p of missingRequiredParameter) {
        errorBlocks.push(Section({ text: SlackCommandService.paramToSlackString(p) }))
      }
    }

    if (invalidParameters.length > 0) {
      if (errorBlocks.length > 0) {
        errorBlocks.push(Divider())
      }
      errorBlocks.push(Section({ text: '*Invalid parameters!*' }))
      errorBlocks.push(Divider())

      for (const p of invalidParameters) {
        errorBlocks.push(Section({ text: SlackCommandService.paramToSlackString(p) }))
      }
    }

    if (errorBlocks.length > 0) {
      return {
        error: SlackCommandService.buildSlackMessage(...errorBlocks),
      }
    }

    return result
  }

  private static findParamValue(
    definition: SlackCommandParameter,
    params: string[],
  ): string | undefined {
    let next = false
    for (const param of params) {
      if (next) {
        return param
      }

      if (param === `--${definition.name}` || param === `-${definition.short}`) {
        if (definition.type === SlackCommandParameterType.BOOLEAN) {
          return 'true'
        }

        next = true
      }
    }

    return undefined
  }

  private static buildSlackMessage(...blocks: any): SlackMessageDto {
    return Message()
      .blocks(...blocks)
      .buildToObject()
  }

  private static paramToSlackString(p: SlackCommandParameter): string {
    return `${p.required ? '*' : ''} _--${p.name}${p.short ? `/-${p.short}` : ''}_ (${p.type}): ${
      p.description
    }${p.default ? `, default: ${p.default}` : ''}${
      p.allowedValues && p.allowedValues.length > 0
        ? `, allowed values: ${p.allowedValues.join(',')}`
        : ''
    }`
  }
}
