import { safeWrap } from '../../middlewares/errorMiddleware'
import { createRateLimiter } from '../apiRateLimiter'

export default (app) => {
  const signInRateLimiter = createRateLimiter({
    max: 100,
    windowMs: 15 * 60 * 1000,
  })

  app.post(`/auth/sign-in`, signInRateLimiter, safeWrap(require('./authSignIn').default))

  const signUpRateLimiter = createRateLimiter({
    max: 20,
    windowMs: 60 * 60 * 1000,
  })

  app.post(`/auth/sign-up`, signUpRateLimiter, safeWrap(require('./authSignUp').default))

  app.put(`/auth/profile`, safeWrap(require('./authUpdateProfile').default))

  app.put(`/auth/change-password`, safeWrap(require('./authPasswordChange').default))

  app.get(`/auth/me`, safeWrap(require('./authMe').default))

  app.post(`/auth/sso/callback`, safeWrap(require('./ssoCallback').default))
}
