import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/groupsio.svg',
  import.meta.url,
).href;

const groupsio: IdentityConfig = {
  key: 'groupsio',
  name: 'Groups.io',
  image,
  member: {
    placeholder: 'Groups.io email address',
  },
  activity: {
    showLink: true,
  },
};

export default groupsio;
