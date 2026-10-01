import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/tnc.png', import.meta.url)
  .href;

const tnc: IdentityConfig = {
  key: 'tnc',
  name: 'Training & Certification',
  image,
  member: {
    placeholder: 'Linux Foundation ID',
  },
};

export default tnc;
