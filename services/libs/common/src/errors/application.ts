export class ApplicationError extends Error {
  constructor(
    message: string,
    public readonly originalError?: Error,
    public readonly metadata?: Record<string, unknown>,
  ) {
    super(message)
    this.name = 'ApplicationError'
  }
}

export class UnrepeatableError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'UnrepeatableError'
  }
}
