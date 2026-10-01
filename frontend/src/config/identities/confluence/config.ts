import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/confluence.svg',
  import.meta.url,
).href;

const confluence: IdentityConfig = {
  key: 'confluence',
  name: 'Confluence',
  image,
  member: {
    placeholder: 'Confluence username or email address',
  },
  activity: {
    showLink: true,
    showSourceId: true,
  },
};

export default confluence;
