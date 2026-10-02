import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/cvent.png', import.meta.url)
  .href;

const cvent: IdentityConfig = {
  key: 'cvent',
  name: 'Cvent',
  image,
  member: {
    placeholder: 'Cvent username or email address',
  },
};

export default cvent;
