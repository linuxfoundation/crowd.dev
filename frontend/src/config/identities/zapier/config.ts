import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/zapier.png', import.meta.url)
  .href;

const zapier: IdentityConfig = {
  key: 'zapier',
  name: 'Zapier',
  image,
};

export default zapier;
