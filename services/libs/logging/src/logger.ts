import * as Bunyan from 'bunyan'
import BunyanFormat from 'bunyan-format'

import { IS_DEV_ENV, IS_TEST_ENV, LOG_LEVEL, SERVICE } from '@crowd/common'

import { Logger } from './types'

const PRETTY_FORMAT = new BunyanFormat({
  outputMode: 'short',
  levelInString: true,
})

const JSON_FORMAT = new BunyanFormat({
  outputMode: 'bunyan',
  levelInString: true,
})

let serviceLoggerInstance: Logger

export function getServiceLogger(): Logger {
  if (serviceLoggerInstance !== undefined) {
    return serviceLoggerInstance
  }

  const usePrettyLogs = IS_DEV_ENV || IS_TEST_ENV || SERVICE === 'script'

  const options: Bunyan.LoggerOptions = {
    name: SERVICE,
    level: LOG_LEVEL as Bunyan.LogLevel,
    stream: usePrettyLogs ? PRETTY_FORMAT : JSON_FORMAT,
    serializers: {
      ...Bunyan.stdSerializers,
      error: Bunyan.stdSerializers.err,
    },
  }

  serviceLoggerInstance = Bunyan.createLogger(options)

  if (!IS_DEV_ENV && !IS_TEST_ENV) {
    delete serviceLoggerInstance.fields.hostname
  }

  return serviceLoggerInstance
}

export const getChildLogger = (
  name: string,
  parent: Logger,
  logProperties?: Record<string, unknown>,
): Logger => {
  const options = {
    component: name,
    ...(logProperties || {}),
  }

  return parent.child(options, true)
}

const serviceChildMap = new Map<string, Logger>()
export const getServiceChildLogger = (
  name: string,
  logProperties?: Record<string, unknown>,
): Logger => {
  if (serviceChildMap.has(name)) return serviceChildMap.get(name)

  const logger = getChildLogger(name, getServiceLogger(), logProperties)
  serviceChildMap.set(name, logger)
  return logger
}
