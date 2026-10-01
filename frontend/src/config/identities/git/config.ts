import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/git.png', import.meta.url)
  .href;

const git: IdentityConfig = {
  key: 'git',
  name: 'Git',
  image,
  member: {
    placeholder: 'Git email address',
  },
  activity: {
    showContentDetails: true,
    showSourceId: true,
    typeIcon: 'commit',
  },
};

export default git;
