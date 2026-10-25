import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/credly.png', import.meta.url).href;

const credly: IdentityConfig = {
  key: 'credly',
  name: 'Credly',
  image,
  member: {
    placeholder: 'Credly email address',
    url: ({ attributes }) => attributes?.url?.credly,
  },
};

export default credly;
