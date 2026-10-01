import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/discord.png', import.meta.url)
  .href;

const discord: IdentityConfig = {
  key: 'discord',
  name: 'Discord',
  image,
  member: {
    placeholder: 'Discord username',
    url: ({ identity }) => (identity.value ? `https://discord.com/${identity.value}` : null),
  },
  activity: {
    showLink: true,
  },
};

export default discord;
