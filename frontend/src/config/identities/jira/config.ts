import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/jira.png', import.meta.url)
  .href;

const jira: IdentityConfig = {
  key: 'jira',
  name: 'Jira',
  image,
  member: {
    placeholder: 'Jira username or email address',
  },
  activity: {
    showLink: true,
    showSourceId: true,
  },
};

export default jira;
