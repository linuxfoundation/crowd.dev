/**
 * @deprecated Use the HTTP error classes in `@libs/common/src/errors/http.ts` instead.
 * Example: `InternalError` instead of `Error500`.
 */
export default class Error500 extends Error {
  code: number

  constructor(message?) {
    message = message || 'Internal server error'
    super(message)
    this.code = 500
  }
}
