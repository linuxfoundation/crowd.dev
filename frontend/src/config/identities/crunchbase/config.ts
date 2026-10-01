import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/crunchbase.png',
  import.meta.url,
).href;

const crunchbase: IdentityConfig = {
  key: 'crunchbase',
  name: 'Crunchbase',
  image,
  organization: {
    urlPrefix: 'https://www.crunchbase.com/organization/',
    handle: (identity) => identity.value,
    url: (identity) => `https://www.crunchbase.com/organization/${identity.value}`,
  },
};

export default crunchbase;
