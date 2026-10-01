import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/gerrit.png', import.meta.url)
  .href;

const gerrit: IdentityConfig = {
  key: 'gerrit',
  name: 'Gerrit',
  image,
  member: {
    placeholder: 'Gerrit username or email address',
  },
  activity: {
    showLink: true,
    showSourceId: true,
  },
};

export default gerrit;
