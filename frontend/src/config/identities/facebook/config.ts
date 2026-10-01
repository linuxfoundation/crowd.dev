import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/facebook.png',
  import.meta.url,
).href;

const facebook: IdentityConfig = {
  key: 'facebook',
  name: 'Facebook',
  image,
};

export default facebook;
