import { Auth0Service } from '@/modules/auth/services/auth0.service';
import { AuthApiService } from '@/modules/auth/services/auth.api.service';
import { AuthService } from '@/modules/auth/services/auth.service';
import { User } from '@/modules/auth/types/User.type';
import Errors from '@/shared/error/errors';
import { disconnectSocket, connectSocket, isSocketConnected } from '@/modules/auth/auth.socket';
import identify from '@/shared/modules/monitoring/identify';
import { watch } from 'vue';
import config from '@/config';
import { setRumUser } from '@/utils/datadog/rum';
import useSessionTracking from '@/shared/modules/monitoring/useSessionTracking';
import { boot as bootIntercom, shutdown as shutdownIntercom } from '@/utils/intercom';

export default {
  init() {
    Auth0Service.isAuthenticated()
      .then((isAuthenticated: boolean) => {
        if (!isAuthenticated) {
          this.handleLocalAuth()
            .catch(() => this.silentLogin());
        } else {
          this.silentLogin();
        }
      });
  },
  setLfxHeader() {
    const lfxHeader = document.getElementById('lfx-header');

    if (!lfxHeader || lfxHeader.authuser) {
      return;
    }

    lfxHeader.docslink = 'https://docs.linuxfoundation.org/lfx/community-management';
    lfxHeader.supportlink = 'https://jira.linuxfoundation.org/plugins/servlet/desk/portal/4?requestGroup=54';

    Auth0Service.getUser().then((user) => {
      if (user) {
        setRumUser(user);
        lfxHeader.authuser = user;

        const intercomJwt = user[config.intercom.auth0IntercomClaim];
        const userId = user[config.intercom.auth0UsernameClaim];
        if (userId && intercomJwt) {
          bootIntercom({
            user_id: userId,
            name: user.name,
            email: user.email,
            intercom_user_jwt: intercomJwt,
          }).catch((error: any) => {
            console.error('Intercom: Boot failed', error);
          });
        }
      }
    });
  },
  silentLogin() {
    Auth0Service.getTokenSilently()
      .then(() => Auth0Service.authData())
      .then((token) => this.authCallback(token))
      .catch((error) => {
        if (['login_required', 'consent_required', 'missing_refresh_token'].includes(error.error)) {
          const appState: any = {};
          if (window.location.href) {
            appState.returnTo = window.location.href.replace(window.location.origin, '');
          }
          return this.signin({ appState });
        }
        return Promise.reject();
      });
  },
  handleLocalAuth() {
    if (['production', 'staging'].includes(config.env)) {
      return Promise.reject();
    }
    const storedToken = AuthService.getToken();
    const params = new URLSearchParams(window.location.search);
    const myJwt = params.get('my-jwt');

    const localToken = storedToken || myJwt;
    if (localToken) {
      return this.getUser(localToken);
    }

    return Promise.reject();
  },
  ensureTrackingSession(): Promise<void> {
    if (!this.loaded) {
      return new Promise((resolve) => {
        const stopWatcher = watch(
          () => this.loaded,
          async (loaded) => {
            if (!loaded) {
              const { startSession, attachListeners } = useSessionTracking();

              this.loaded = true;

              await startSession();
              attachListeners();
            }

            resolve();
            stopWatcher();
          },
          {
            immediate: true,
          },
        );
      });
    }

    return Promise.resolve();
  },
  ensureLoaded(): Promise<void> {
    if (!this.user) {
      return new Promise((resolve) => {
        const stopWatcher = watch(
          () => this.user,
          async (newUser) => {
            if (newUser) {
              this.setLfxHeader();

              resolve();
              stopWatcher();
            }
          },
          {
            immediate: true,
          },
        );
      });
    }
    // Both are already loaded
    return Promise.resolve();
  },
  getUser(token?: string) {
    const t = token || AuthService.getToken();
    if (!t) {
      return Promise.reject();
    }
    if (!isSocketConnected()) {
      connectSocket(t);
    }
    AuthService.setToken(t);
    return AuthApiService.fetchMe()
      .then((user) => {
        this.user = user;
        identify(user);
        this.setLfxHeader();

        return Promise.resolve(user);
      })
      .catch((error) => {
        AuthService.logout();
        Auth0Service.logout();
        Errors.handle(error);
        return Promise.reject(error);
      });
  },
  authCallback(token: string | null): Promise<User> {
    if (!token) {
      return Promise.reject();
    }
    return AuthApiService.ssoGetToken(token)
      .then((token) => this.getUser(token))
      .catch(() => this.logout());
  },
  signin(params?: any) {
    return Auth0Service.loginWithRedirect(params);
  },
  logout() {
    disconnectSocket();
    shutdownIntercom();
    this.user = null;
    return Auth0Service.logout();
  },
};
