import type { IntegrationConfig } from '@/config/integrations';

const image = new URL('@/assets/images/integrations/credly.png', import.meta.url).href;

const credly: IntegrationConfig = {
  key: 'credly',
  name: 'Credly',
  image,
  description:
    'Badges issued by the Linux Foundation on Credly: certifications, courses and recognitions, matched to projects automatically.',
  showProgress: false,
  autoManaged: true,
};

export default credly;
