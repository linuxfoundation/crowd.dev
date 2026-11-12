// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { defineStore } from 'pinia';
import { computed, ref, watch } from 'vue';
import type { RedirectLoginOptions } from '@auth0/auth0-spa-js';
import { Auth0Service } from '@/modules/auth/services/auth0.service';
import { AuthApiService } from '@/modules/auth/services/auth.api.service';
import { AuthService } from '@/modules/auth/services/auth.service';
import type { User } from '@/modules/auth/types/User.type';
import type { TenantUser } from '@/modules/auth/types/TenantUser.type';
import type { Tenant } from '@/modules/auth/types/Tenant.type';
import type { Auth0UserWithLfClaims } from '@/modules/auth/types/Auth0UserWithLfClaims.type';
import type { LfxHeaderElement } from '@/modules/auth/types/LfxHeaderElement.type';
import { LfRole } from '@/shared/modules/permissions/types/Roles';
import Errors from '@/shared/error/errors';
import { disconnectSocket, connectSocket, isSocketConnected } from '@/modules/auth/auth.socket';
import identify from '@/shared/modules/monitoring/identify';
import config from '@/config';
import { setRumUser } from '@/utils/datadog/rum';
import useSessionTracking from '@/shared/modules/monitoring/useSessionTracking';
import { boot as bootIntercom, shutdown as shutdownIntercom } from '@/utils/intercom';

export const useAuthStore = defineStore('auth', () => {
  const user = ref<User | null>(null);
  const loaded = ref(false);

  const tenantUser = computed<TenantUser | null>(() => user.value?.tenants?.[0] || null);
  const tenant = computed<Tenant | null>(() => tenantUser.value?.tenant || null);
  const roles = computed<LfRole[]>(() => tenantUser.value?.roles || [LfRole.readonly]);
  const isEmailVerified = computed<boolean>(() => !!user.value?.id && user.value.emailVerified);

  function setLfxHeader() {
    const lfxHeader = document.getElementById('lfx-header') as LfxHeaderElement | null;

    if (!lfxHeader || lfxHeader.authuser) {
      return;
    }

    lfxHeader.docslink = 'https://docs.linuxfoundation.org/lfx/community-management';
    lfxHeader.supportlink =
      'https://jira.linuxfoundation.org/plugins/servlet/desk/portal/4?requestGroup=54';

    Auth0Service.getUser().then((auth0User?: Auth0UserWithLfClaims) => {
      if (auth0User) {
        setRumUser(auth0User);
        lfxHeader.authuser = auth0User;

        const intercomJwt = auth0User[config.intercom.auth0IntercomClaim];
        const userId = auth0User[config.intercom.auth0UsernameClaim];
        if (userId && intercomJwt) {
          bootIntercom({
            user_id: userId as string,
            name: auth0User.name,
            email: auth0User.email,
            intercom_user_jwt: intercomJwt as string,
          }).catch((error: unknown) => {
            console.error('Intercom: Boot failed', error);
          });
        }
      }
    });
  }

  function getUser(token?: string) {
    const t = token || AuthService.getToken();
    if (!t) {
      return Promise.reject();
    }
    if (!isSocketConnected()) {
      connectSocket(t, () => user.value);
    }
    AuthService.setToken(t);
    return AuthApiService.fetchMe()
      .then((fetchedUser) => {
        user.value = fetchedUser;
        identify(fetchedUser);
        setLfxHeader();

        return Promise.resolve(fetchedUser);
      })
      .catch((error) => {
        AuthService.logout();
        Auth0Service.logout();
        Errors.handle(error);
        return Promise.reject(error);
      });
  }

  function logout() {
    disconnectSocket();
    shutdownIntercom();
    user.value = null;
    return Auth0Service.logout();
  }

  function authCallback(token: string | null): Promise<User | void> {
    if (!token) {
      return Promise.reject();
    }
    return AuthApiService.ssoGetToken(token)
      .then((ssoToken) => getUser(ssoToken))
      .catch(() => logout());
  }

  function signin(params?: RedirectLoginOptions) {
    return Auth0Service.loginWithRedirect(params);
  }

  function silentLogin() {
    Auth0Service.getTokenSilently()
      .then(() => Auth0Service.authData())
      .then((token) => authCallback(token))
      .catch((error) => {
        if (['login_required', 'consent_required', 'missing_refresh_token'].includes(error.error)) {
          const appState: { returnTo?: string } = {};
          if (window.location.href) {
            appState.returnTo = window.location.href.replace(window.location.origin, '');
          }
          return signin({ appState });
        }
        return Promise.reject();
      });
  }

  function handleLocalAuth() {
    if (['production', 'staging'].includes(config.env!)) {
      return Promise.reject();
    }
    const localToken =
      AuthService.getToken() || new URLSearchParams(window.location.search).get('my-jwt');
    if (localToken) {
      return getUser(localToken);
    }

    return Promise.reject();
  }

  function init() {
    Auth0Service.isAuthenticated().then((isAuthenticated) => {
      if (!isAuthenticated) {
        handleLocalAuth().catch(() => silentLogin());
      } else {
        silentLogin();
      }
    });
  }

  function ensureTrackingSession(): Promise<void> {
    if (!loaded.value) {
      return new Promise((resolve) => {
        const stopWatcher = watch(
          loaded,
          async (isLoaded) => {
            if (!isLoaded) {
              const { startSession, attachListeners } = useSessionTracking(() => user.value);

              loaded.value = true;

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
  }

  function ensureLoaded(): Promise<void> {
    if (!user.value) {
      return new Promise((resolve) => {
        const stopWatcher = watch(
          user,
          async (newUser) => {
            if (newUser) {
              setLfxHeader();

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
    return Promise.resolve();
  }

  return {
    user,
    loaded,
    tenantUser,
    tenant,
    roles,
    isEmailVerified,
    init,
    setLfxHeader,
    silentLogin,
    handleLocalAuth,
    ensureTrackingSession,
    ensureLoaded,
    getUser,
    authCallback,
    signin,
    logout,
  };
});
