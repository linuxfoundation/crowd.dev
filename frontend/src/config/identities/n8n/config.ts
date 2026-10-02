import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/n8n.svg', import.meta.url)
  .href;

const n8n: IdentityConfig = {
  key: 'n8n',
  name: 'n8n',
  image,
};

export default n8n;
