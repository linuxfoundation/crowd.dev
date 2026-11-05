/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable no-use-before-define */
import config from '@/config';

interface IntercomStub {
  (...args: unknown[]): void;
  q?: unknown[][];
  c?: (args: unknown[]) => void;
}

declare global {
  interface Window {
    Intercom?: IntercomStub;
    intercomSettings?: Record<string, unknown>;
  }
}

let isLoaded = false;
let isBooted = false;
let isLoading = false;

export interface IntercomBootOptions {
  user_id: string;
  name?: string;
  email?: string;
  intercom_user_jwt?: string;
}

const loadScript = (): void => {
  if (isLoaded || isLoading || typeof window === 'undefined') {
    return;
  }
  isLoading = true;

  // Create stub so queued commands work before script loads
  const ic = window.Intercom;
  if (typeof ic === 'function') {
    ic('reattach_activator');
    ic('update', window.intercomSettings);
  } else {
    const queue: unknown[][] = [];
    const i: IntercomStub = (...args: unknown[]) => { i.c?.(args); };
    i.q = queue;
    i.c = (args: unknown[]) => { queue.push(args); };
    window.Intercom = i;
  }

  // Pre-set app settings
  window.intercomSettings = {
    api_base: config.intercom.apiBase,
    app_id: config.intercom.appId,
  };

  const script = document.createElement('script');
  script.type = 'text/javascript';
  script.async = true;
  script.src = `https://widget.intercom.io/widget/${config.intercom.appId}`;
  script.onload = () => {
    isLoaded = true;
    isLoading = false;
  };
  script.onerror = (error) => {
    isLoading = false;
    console.error('Intercom: Failed to load script', error);
  };

  const firstScript = document.getElementsByTagName('script')[0];
  if (firstScript?.parentNode) {
    firstScript.parentNode.insertBefore(script, firstScript);
  } else {
    (document.head || document.body).appendChild(script);
  }
};

export const boot = (options: IntercomBootOptions): Promise<void> => new Promise((resolve, reject) => {
  if (typeof window === 'undefined') {
    reject(new Error('Window is undefined'));
    return;
  }

  if (!config.intercom.appId) {
    console.info('Intercom: Disabled (no appId configured)');
    reject(new Error('No Intercom app ID configured'));
    return;
  }

  if (isBooted) {
    const { intercom_user_jwt: _jwt, ...updateOptions } = options;
    update(updateOptions);
    resolve();
    return;
  }

  if (!isLoaded && !isLoading) {
    loadScript();
  }

  // Set JWT in intercomSettings before boot — required for identity verification
  if (options.intercom_user_jwt) {
    window.intercomSettings = window.intercomSettings || {};
    window.intercomSettings.intercom_user_jwt = options.intercom_user_jwt;
  }

  const checkLoaded = setInterval(() => {
    if (isLoaded && window.Intercom) {
      clearInterval(checkLoaded);
      clearTimeout(timeoutHandle);

      if (isBooted) {
        const { intercom_user_jwt: _jwt, ...updateOptions } = options;
        update(updateOptions);
        resolve();
        return;
      }

      isBooted = true;
      try {
        const { intercom_user_jwt: _jwt, ...bootOptions } = options;
        window.Intercom('boot', {
          api_base: config.intercom.apiBase,
          app_id: config.intercom.appId,
          ...bootOptions,
        });
        resolve();
      } catch (error) {
        isBooted = false;
        console.error('Intercom: Boot failed', error);
        reject(error);
      }
    }
  }, 100);

  const timeoutHandle = setTimeout(() => {
    clearInterval(checkLoaded);
    if (!isBooted) {
      isLoading = false;
      reject(new Error('Intercom script failed to load'));
    }
  }, 10000);
});

export const update = (data?: Partial<IntercomBootOptions>): void => {
  if (typeof window !== 'undefined' && window.Intercom && isBooted) {
    try {
      window.Intercom('update', data || {});
    } catch (error) {
      console.error('Intercom: Update failed', error);
    }
  }
};

export const shutdown = (): void => {
  if (typeof window === 'undefined') {
    return;
  }
  if (window.intercomSettings?.intercom_user_jwt) {
    delete window.intercomSettings.intercom_user_jwt;
  }
  if (window.Intercom && isBooted) {
    try {
      window.Intercom('shutdown');
      isBooted = false;
    } catch (error) {
      console.error('Intercom: Shutdown failed', error);
    }
  }
};

export const useIntercom = () => ({
  boot,
  update,
  shutdown,
});
