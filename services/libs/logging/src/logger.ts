import * as Bunyan from 'bunyan'
import BunyanFormat from 'bunyan-format'

import { IS_DEV_ENV, IS_TEST_ENV, LOG_LEVEL, SERVICE } from '@crowd/common'

import { Logger } from './types'

type SerializableError = {
  toJSON?: () => unknown
}

function serializeError(err: unknown) {
  if (err && typeof err === 'object') {
    const toJSON = (err as SerializableError).toJSON

    if (typeof toJSON === 'function') {
      try {
        const serialized = toJSON.call(err)

        if (serialized !== undefined) {
          return serialized
        }
      } catch {
        // Fall back to Bunyan's standard error serialization.
      }
    }
  }

  return Bunyan.stdSerializers.err(err)
}

const serializers = {
  ...Bunyan.stdSerializers,
  err: serializeError,
  error: serializeError,
}

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
    serializers,
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
