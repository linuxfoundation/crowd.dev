import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/discourse.png',
  import.meta.url,
).href;

const discourse: IdentityConfig = {
  key: 'discourse',
  name: 'Discourse',
  image,
  member: {
    urlPrefix: 'https://meta.discourse.org/u/',
    url: ({ attributes }) => attributes?.url?.discourse,
  },
  activity: {
    showLink: true,
  },
};

export default discourse;
