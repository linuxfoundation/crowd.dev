// Copyright (c) 2025 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

declare global {
  interface Window {
    analytics?: any;
  }
}

const SEGMENT_SNIPPET_VERSION = '4.1.0';

const ANALYTICS_METHODS = [
  'trackSubmit',
  'trackClick',
  'trackLink',
  'trackForm',
  'pageview',
  'identify',
  'reset',
  'group',
  'track',
  'ready',
  'alias',
  'debug',
  'page',
  'once',
  'off',
  'on',
];

export const installAnalyticsStub = (): void => {
  const analytics = window.analytics || [];
  window.analytics = analytics;

  if (analytics.initialize || analytics.invoked) {
    return;
  }

  analytics.invoked = true;
  analytics.methods = ANALYTICS_METHODS;
  analytics.factory = (method: string) => (...args: any[]) => {
    args.unshift(method);
    analytics.push(args);
    return analytics;
  };

  ANALYTICS_METHODS.forEach((method) => {
    analytics[method] = analytics.factory(method);
  });

  analytics.SNIPPET_VERSION = SEGMENT_SNIPPET_VERSION;
};

export const loadSegment = (key: string): void => {
  const { analytics } = window;
  if (!analytics) {
    return;
  }

  analytics.load = (writeKey: string) => {
    const script = document.createElement('script');
    script.type = 'text/javascript';
    script.async = true;
    script.src = `https://cdn.segment.com/analytics.js/v1/${writeKey}/analytics.min.js`;

    const firstScript = document.getElementsByTagName('script')[0];
    firstScript.parentNode?.insertBefore(script, firstScript);
  };

  analytics.load(key);
};

export const isRealSegmentKey = (key?: string): boolean => {
  if (!key) {
    return false;
  }

  return !key.startsWith('CROWD_VUE_APP') && !key.startsWith('%VUE_APP');
};
