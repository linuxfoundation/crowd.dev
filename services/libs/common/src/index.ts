import { ApplicationError, UnrepeatableError } from './errors/application'
import Error400 from './errors/deprecated/Error400'
import Error401 from './errors/deprecated/Error401'
import Error403 from './errors/deprecated/Error403'
import Error404 from './errors/deprecated/Error404'
import Error405 from './errors/deprecated/Error405'
import Error409 from './errors/deprecated/Error409'
import Error500 from './errors/deprecated/Error500'
import Error542 from './errors/deprecated/Error542'
import {
  BadRequestError,
  ConflictError,
  ForbiddenError,
  HttpError,
  InsufficientScopeError,
  InternalError,
  NotFoundError,
  RateLimitError,
  UnauthorizedError,
} from './errors/http'

export * from './env'
export * from './timing'
export * from './utils'
export * from './array'
export * from './object'
export * from './uuidUtils'
export * from './validations'
export * from './strings'
export * from './types'
export * from './requestThrottler'
export * from './rawQueryParser'
export * from './byteLength'
export * from './domain'
export * from './displayName'
export * from './jira'
export * from './email'
export * from './bot'

export * from './i18n'
export * from './member'
export * from './crypto'

export {
  // Legacy errors
  Error400,
  Error401,
  Error403,
  Error404,
  Error405,
  Error409,
  Error500,
  Error542,

  // HTTP errors
  HttpError,
  BadRequestError,
  UnauthorizedError,
  ForbiddenError,
  InsufficientScopeError,
  NotFoundError,
  ConflictError,
  RateLimitError,
  InternalError,

  // Application errors
  ApplicationError,
  UnrepeatableError,
}
