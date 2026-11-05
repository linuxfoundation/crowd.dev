import config from '@/config';
import type { RedirectLoginOptions } from '@auth0/auth0-spa-js';
import { Auth0Client } from '@auth0/auth0-spa-js';

const baseUrl = `${config.frontendUrl.protocol}://${config.frontendUrl.host}`;
const authCallback = `${baseUrl}/auth/callback`;

const scope: string = 'profile email';

class Auth0ServiceClass {
  private readonly webAuth: Auth0Client;

  public constructor() {
    this.webAuth = new Auth0Client({
      domain: config.auth0.domain,
      clientId: config.auth0.clientId,
      authorizationParams: {
        redirect_uri: authCallback,
        scope,
      },
      useCookiesForTransactions: true,
      useRefreshTokens: false,
    });
  }

  loginWithRedirect(params?: RedirectLoginOptions) {
    return this.webAuth.loginWithRedirect({
      ...params,
      authorizationParams: {
        scope,
      },
    });
  }

  handleAuth() {
    return this.webAuth.handleRedirectCallback();
  }

  isAuthenticated() {
    return this.webAuth.isAuthenticated();
  }

  getTokenSilently() {
    return this.webAuth.getTokenSilently();
  }

  authData() {
    return this.webAuth.getIdTokenClaims().then((idToken) => {
      if (idToken) {
        // eslint-disable-next-line no-underscore-dangle
        return idToken.__raw;
      }

      return null;
    });
  }

  public logout() {
    return this.webAuth.logout();
  }

  public getUser() {
    return this.webAuth.getUser();
  }
}

export const Auth0Service = new Auth0ServiceClass();
