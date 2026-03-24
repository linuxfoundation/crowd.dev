import { IS_PROD_ENV } from '../../env'
import { i18n, i18nExists } from '../../i18n'

/**
 * @deprecated Use the HTTP error classes in `@libs/common/src/errors/http.ts` instead.
 * Example: `BadRequestError` instead of `Error400`.
 */
export default class Error400 extends Error {
  code: number

  constructor(language?, messageCode?, ...args) {
    let message

    console.log('Error400: constructor:', {
      language,
      messageCode,
      args,
      IS_PROD_ENV,
      i18nExists: i18nExists(language, messageCode),
    })

    if (messageCode && (!IS_PROD_ENV || i18nExists(language, messageCode))) {
      message = i18n(language, messageCode, ...args)
    }

    console.log('Error400: constructor p1:', { message })

    message = message || i18n(language, 'errors.validation.message')

    console.log('Error400: constructor p2:', { message })

    super(message)
    this.code = 400
  }
}
