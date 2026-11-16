import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/luma.png', import.meta.url).href;

const luma: IdentityConfig = {
  key: 'luma',
  name: 'Luma',
  image,
  member: {
    placeholder: 'Luma username or email address',
  },
};

export default luma;
