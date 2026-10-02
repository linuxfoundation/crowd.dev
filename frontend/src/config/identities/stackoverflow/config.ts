import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/stackoverflow.png',
  import.meta.url,
).href;

const stackoverflow: IdentityConfig = {
  key: 'stackoverflow',
  name: 'Stack Overflow',
  image,
  member: {
    urlPrefix: 'stackoverflow.com/users/',
    url: ({ attributes }) => attributes?.url?.stackoverflow || null,
  },
  activity: {
    showLink: true,
  },
};

export default stackoverflow;
