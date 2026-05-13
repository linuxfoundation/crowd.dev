import { i18n, i18nExists } from '../../i18n'

/**
 * @deprecated Use the HTTP error classes in `@libs/common/src/errors/http.ts` instead.
 * Example: `UnauthorizedError` instead of `Error401`.
 */
export default class Error401 extends Error {
  code: number

  constructor(language?, messageCode?, ...args) {
    let message

    if (messageCode && i18nExists(language, messageCode)) {
      message = i18n(language, messageCode, ...args)
    }

    message = message || i18n(language, 'errors.validation.message')

    super(message)
    this.code = 401
  }
}
