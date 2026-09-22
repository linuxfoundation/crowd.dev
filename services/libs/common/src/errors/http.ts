/**
 * Base class for HTTP errors with structured JSON responses.
 * Subclasses must define a `code` and `status`.
 */
export abstract class HttpError extends Error {
  abstract readonly code: string
  abstract readonly status: number
  readonly context?: Record<string, unknown>

  constructor(message: string, context?: Record<string, unknown>) {
    super(message)
    this.name = this.constructor.name
    this.context = context
    Object.setPrototypeOf(this, new.target.prototype)
  }

  toJSON() {
    return {
      error: {
        code: this.code,
        message: this.message,
        ...(this.context ? { context: this.context } : {}),
      },
    }
  }
}

export class BadRequestError extends HttpError {
  readonly code = 'BAD_REQUEST'
  readonly status = 400

  constructor(message = 'Bad request', context?: Record<string, unknown>) {
    super(message, context)
  }
}

export class UnauthorizedError extends HttpError {
  readonly code = 'UNAUTHORIZED'
  readonly status = 401

  constructor(message = 'Unauthorized', context?: Record<string, unknown>) {
    super(message, context)
  }
}

export class ForbiddenError extends HttpError {
  readonly code = 'FORBIDDEN'
  readonly status = 403

  constructor(message = 'Forbidden', context?: Record<string, unknown>) {
    super(message, context)
  }
}

export class InsufficientScopeError extends HttpError {
  readonly code = 'INSUFFICIENT_SCOPE'
  readonly status = 403

  constructor(
    message = 'Insufficient scope for this operation',
    context?: Record<string, unknown>,
  ) {
    super(message, context)
  }
}

export class NotFoundError extends HttpError {
  readonly code = 'NOT_FOUND'
  readonly status = 404

  constructor(message = 'Not found', context?: Record<string, unknown>) {
    super(message, context)
  }
}

export class ConflictError extends HttpError {
  readonly code = 'CONFLICT'
  readonly status = 409

  constructor(message = 'Conflict', context?: Record<string, unknown>) {
    super(message, context)
  }
}

export class RateLimitError extends HttpError {
  readonly code = 'RATE_LIMITED'
  readonly status = 429

  constructor(
    message = 'Too many requests, please try again later',
    context?: Record<string, unknown>,
  ) {
    super(message, context)
  }
}

export class InternalError extends HttpError {
  readonly code = 'INTERNAL_ERROR'
  readonly status = 500

  constructor(message = 'Internal server error', context?: Record<string, unknown>) {
    super(message, context)
  }
}
