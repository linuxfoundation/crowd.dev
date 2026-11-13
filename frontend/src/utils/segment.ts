// Copyright (c) 2025 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

const LFX_SEGMENT_URL =
  'https://lfx-segment.platform.linuxfoundation.org/latest/lfx-segment-analytics.min.js';

type EventProperties = Record<string, unknown>;

interface LfxSegmentsAnalytics {
  init: (writeKey?: string) => Promise<void>;
  track: (event: string, properties?: EventProperties) => Promise<void>;
  page: (name?: string, properties?: EventProperties) => Promise<void>;
  identify: (userId: string | null, traits?: EventProperties) => Promise<void>;
  reset: () => Promise<void>;
}

declare global {
  interface Window {
    LfxAnalytics?: {
      LfxSegmentsAnalytics?: { getInstance: () => LfxSegmentsAnalytics };
    };
  }
}

let lfxAnalytics: Promise<LfxSegmentsAnalytics | null> = Promise.resolve(null);

const injectScript = (src: string): Promise<void> =>
  new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = src;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`Failed to load ${src}`));
    document.head.appendChild(script);
  });

const loadLfxAnalytics = async (writeKey: string): Promise<LfxSegmentsAnalytics | null> => {
  await injectScript(LFX_SEGMENT_URL);
  const instance = window.LfxAnalytics?.LfxSegmentsAnalytics?.getInstance();
  if (!instance) {
    console.error('Segment library loaded without LfxSegmentsAnalytics');
    return null;
  }
  // init() must be called before any queued call reaches the instance, or that call is dropped as held.
  instance.init(writeKey).catch((error: unknown) => console.error('Segment failed to load', error));
  return instance;
};

export const loadSegment = (writeKey: string): void => {
  lfxAnalytics = loadLfxAnalytics(writeKey).catch(() => null);
};

// No call site awaits or catches these calls, so a failure is logged here instead of becoming an unhandled rejection.
const withAnalytics = (send: (instance: LfxSegmentsAnalytics) => Promise<void>): Promise<void> =>
  lfxAnalytics
    .then((instance) => (instance ? send(instance) : undefined))
    .catch((error: unknown) => console.error('Segment call failed', error));

// The visitor identity analytics.js stores, as the LFX library purges it: these cookies on the host and every parent
// domain, and these localStorage keys.
const SEGMENT_IDENTITY_COOKIES = ['ajs_anonymous_id', 'ajs_user_id', 'ajs_group_id'];
const SEGMENT_IDENTITY_STORAGE_KEYS = [
  ...SEGMENT_IDENTITY_COOKIES,
  'ajs_user_traits',
  'ajs_group_properties',
];

// Sign-out can't wait for the library: one still loading resets only after the Auth0 redirect has left the page, and
// releases before v0.0.14 can't reset before their SDK is up.
const forgetStoredSegmentIdentity = (): void => {
  try {
    SEGMENT_IDENTITY_STORAGE_KEYS.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // Storage is blocked, so nothing was stored there.
  }
  const labels = window.location.hostname.split('.');
  const domains = [''];
  for (let i = 0; i < labels.length - 1; i += 1) {
    domains.push(`; Domain=.${labels.slice(i).join('.')}`);
  }
  SEGMENT_IDENTITY_COOKIES.forEach((name) => {
    domains.forEach((domain) => {
      document.cookie = `${name}=; Path=/; Max-Age=0${domain}`;
    });
  });
};

export const analytics = {
  track: (event: string, properties?: EventProperties) =>
    withAnalytics((instance) => instance.track(event, properties)),
  page: (name: string) => withAnalytics((instance) => instance.page(name)),
  identify: (userId: string, traits: EventProperties) =>
    withAnalytics((instance) => instance.identify(userId, traits)),
  reset: () => {
    forgetStoredSegmentIdentity();
    return withAnalytics((instance) => instance.reset());
  },
};

export const isRealSegmentKey = (key?: string): boolean => {
  if (!key) {
    return false;
  }

  return !key.startsWith('CROWD_VUE_APP') && !key.startsWith('%VUE_APP');
};
