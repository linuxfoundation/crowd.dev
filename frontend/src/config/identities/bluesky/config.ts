import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/bluesky.svg', import.meta.url).href;

const bluesky: IdentityConfig = {
  key: 'bluesky',
  name: 'Bluesky',
  image,
  member: {
    urlPrefix: 'bsky.app/profile/',
    url: ({ identity }) =>
      identity.value ? `https://bsky.app/profile/${identity.value.replace(/^@/, '')}` : null,
  },
  activity: {
    showLink: true,
  },
};

export default bluesky;
