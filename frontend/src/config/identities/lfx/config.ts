import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/lfx.png', import.meta.url)
  .href;

const lfid: IdentityConfig = {
  key: 'lfid',
  name: 'Linux Foundation ID',
  image,
  member: {
    placeholder: 'Linux Foundation ID',
  },
};

export default lfid;
