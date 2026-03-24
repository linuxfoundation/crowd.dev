import { i18n, i18nExists } from '../../i18n'

/**
 * @deprecated Use the HTTP error classes in `@libs/common/src/errors/http.ts` instead.
 * Example: `MethodNotAllowedError` instead of `Error405`.
 */
export default class Error405 extends Error {
  code: number

  constructor(language?, messageCode?) {
    let message

    if (messageCode && i18nExists(language, messageCode)) {
      message = i18n(language, messageCode)
    }

    message = message || i18n(language, 'errors.notFound.message')

    super(message)
    this.code = 405
  }
}
