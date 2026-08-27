export type ErrorClass =
  | 'provider.unavailable'
  | 'provider.rate_limit'
  | 'provider.auth'
  | 'provider.contract'
  | 'connector.code'
  | 'sink.rejected'
  | 'unknown'

export interface ConnectorErrorOptions {
  status?: number
  resumeAt?: Date
  cause?: unknown
}

export class ConnectorError extends Error {
  constructor(
    readonly errorClass: ErrorClass,
    message: string,
    readonly options?: ConnectorErrorOptions,
  ) {
    super(message)
    this.name = 'ConnectorError'
  }
}

export class ProviderUnavailableError extends ConnectorError {
  constructor(message = 'provider unavailable', options?: ConnectorErrorOptions) {
    super('provider.unavailable', message, options)
    this.name = 'ProviderUnavailableError'
  }
}

export class RateLimitError extends ConnectorError {
  constructor(message = 'rate limited by provider', options?: ConnectorErrorOptions) {
    super('provider.rate_limit', message, options)
    this.name = 'RateLimitError'
  }
}

export class ProviderAuthError extends ConnectorError {
  constructor(message = 'provider authentication failed', options?: ConnectorErrorOptions) {
    super('provider.auth', message, options)
    this.name = 'ProviderAuthError'
  }
}

export class ProviderContractError extends ConnectorError {
  constructor(message = 'unexpected provider response', options?: ConnectorErrorOptions) {
    super('provider.contract', message, options)
    this.name = 'ProviderContractError'
  }
}

export class ConnectorCodeError extends ConnectorError {
  constructor(message = 'connector code error', options?: ConnectorErrorOptions) {
    super('connector.code', message, options)
    this.name = 'ConnectorCodeError'
  }
}

export function errorFromHttpStatus(
  status: number | undefined,
  message?: string,
  options?: ConnectorErrorOptions,
): ConnectorError {
  const opts = { ...options, status }
  if (status === undefined) {
    return new ProviderUnavailableError(message ?? 'no response from provider', opts)
  }
  if (status === 401 || status === 403) {
    return new ProviderAuthError(message ?? `provider returned status ${status}`, opts)
  }
  if (status === 429) {
    return new RateLimitError(message ?? `provider returned status ${status}`, opts)
  }
  if (status >= 500) {
    return new ProviderUnavailableError(message ?? `provider returned status ${status}`, opts)
  }
  if (status >= 400) {
    return new ProviderContractError(message ?? `provider returned status ${status}`, opts)
  }
  return new ConnectorError('unknown', message ?? `unexpected status ${status}`, opts)
}
