import { datadogRum } from '@datadog/browser-rum';
import type { User } from '@auth0/auth0-spa-js';
import config from '@/config';

export function initRUM(): void {
  datadogRum.init({
    applicationId: config.datadog.rum.applicationId!,
    clientToken: config.datadog.rum.clientToken!,
    site: 'datadoghq.com',
    service: 'lfx-cm-(crowd)',
    env: 'dev',
    // Specify a version number to identify the deployed version of your application in Datadog
    // version: '1.0.0',
    sessionSampleRate: 100,
    sessionReplaySampleRate: 100,
    trackUserInteractions: true,
    trackResources: true,
    trackLongTasks: true,
    defaultPrivacyLevel: 'allow',
  });
}

export function setRumUser(user: User): void {
  datadogRum.setUser({
    id: user['https://sso.linuxfoundation.org/claims/username'],
    email: user.email,
    name: user.name,
  });
}
