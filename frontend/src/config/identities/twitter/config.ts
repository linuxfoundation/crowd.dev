import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/twitter.png', import.meta.url)
  .href;

const twitter: IdentityConfig = {
  key: 'twitter',
  name: 'X/Twitter',
  image,
  member: {
    urlPrefix: 'twitter.com/',
    url: ({ identity }) => (identity.value ? `https://twitter.com/${identity.value}` : null),
  },
  organization: {
    urlPrefix: 'twitter.com/',
  },
  activity: {
    showLink: true,
  },
};

export default twitter;
