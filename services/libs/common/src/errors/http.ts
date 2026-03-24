/**
 * Base class for HTTP errors with structured JSON responses.
 * Subclasses must define a `code` and `status`.
 */
export abstract class HttpError extends Error {
  abstract readonly code: string
  abstract readonly status: number

  constructor(message: string) {
    super(message)
    this.name = this.constructor.name
    Object.setPrototypeOf(this, new.target.prototype)
  }

  toJSON() {
    return {
      error: {
        code: this.code,
        message: this.message,
      },
    }
  }
}

export class BadRequestError extends HttpError {
  readonly code = 'BAD_REQUEST'
  readonly status = 400

  constructor(message = 'Bad request') {
    super(message)
  }
}

export class UnauthorizedError extends HttpError {
  readonly code = 'UNAUTHORIZED'
  readonly status = 401

  constructor(message = 'Unauthorized') {
    super(message)
  }
}

export class ForbiddenError extends HttpError {
  readonly code = 'FORBIDDEN'
  readonly status = 403

  constructor(message = 'Forbidden') {
    super(message)
  }
}

export class InsufficientScopeError extends HttpError {
  readonly code = 'INSUFFICIENT_SCOPE'
  readonly status = 403

  constructor(message = 'Insufficient scope for this operation') {
    super(message)
  }
}

export class NotFoundError extends HttpError {
  readonly code = 'NOT_FOUND'
  readonly status = 404

  constructor(message = 'Not found') {
    super(message)
  }
}

export class ConflictError extends HttpError {
  readonly code = 'CONFLICT'
  readonly status = 409

  constructor(message = 'Conflict') {
    super(message)
  }
}

export class RateLimitError extends HttpError {
  readonly code = 'RATE_LIMITED'
  readonly status = 429

  constructor(message = 'Too many requests, please try again later') {
    super(message)
  }
}

export class InternalError extends HttpError {
  readonly code = 'INTERNAL_ERROR'
  readonly status = 500

  constructor(message = 'Internal server error') {
    super(message)
  }
}
