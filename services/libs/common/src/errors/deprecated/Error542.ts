/**
 * @deprecated Use the HTTP error classes in `@libs/common/src/errors/http.ts` instead.
 * Example: `UnprocessableEntityError` instead of `Error542`.
 */
export default class Error542 extends Error {
  code: number

  constructor(message?) {
    message = message || 'Internal server error'
    super(message)
    this.code = 542
  }
}
