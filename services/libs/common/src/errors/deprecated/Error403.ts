import { IS_PROD_ENV } from '../../env'
import { i18n, i18nExists } from '../../i18n'

/**
 * @deprecated Use the HTTP error classes in `@libs/common/src/errors/http.ts` instead.
 * Example: `ForbiddenError` instead of `Error403`.
 */
export default class Error403 extends Error {
  code: number

  constructor(language?, messageCode?, params?) {
    let message

    // This production check is to allow us arbitrary error messages in development
    if (messageCode && (!IS_PROD_ENV || i18nExists(language, messageCode))) {
      message = i18n(language, messageCode)
    }

    message = message || i18n(language, 'errors.forbidden.message')

    if (params && params.integration && params.scopes) {
      if (typeof params.scopes === 'string') {
        params.scopes = [params.scopes.join(', ')]
      }
      message = message.replace('{integration}', params.integration)
      message = message.replace('{scopes}', params.scopes)
    }

    super(message)
    this.code = 403
  }
}
