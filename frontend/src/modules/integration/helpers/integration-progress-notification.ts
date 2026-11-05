import { createVNode, h } from 'vue';
import AppIntegrationProgress from '@/modules/integration/components/integration-progress.vue';
import AppIntegrationProgressWrapper from '@/modules/integration/components/integration-progress-wrapper.vue';
import { lfIntegrations } from '@/config/integrations';
import { ToastStore } from '@/shared/message/notification';
import type { IntegrationProgress } from '@/modules/integration/types/IntegrationProgress';

export const showIntegrationProgressNotification = (platform: string, segmentId: string) => {
  const label = lfIntegrations()[platform]?.name;
  const defaultSlot = ({ progress }: { progress: IntegrationProgress[] | null }) => {
    const integrationProgress = (progress || []).find((p: IntegrationProgress) => p.platform === platform);
    const text = h('p', {
      class: 'text-2xs text-black leading-4.5 pb-4',
    }, 'Sit back and relax. We will send you an email when the entire process is completed.');
    const fallback = h('p', {
      class: 'text-2xs text-gray-500 leading-4',
    }, integrationProgress ? 'Calculating...' : 'Loading progress...');
    if (!integrationProgress || integrationProgress.reportStatus === 'calculating') {
      return [text, fallback];
    }
    const progressComponentVNode = createVNode(AppIntegrationProgress, {
      progress: integrationProgress,
      showBar: true,
      showParts: true,
    }, {
      default: () => [text],
    });

    return [progressComponentVNode];
  };

  const wrapper = createVNode(AppIntegrationProgressWrapper, {
    segments: [segmentId],
  }, {
    default: defaultSlot,
  });

  ToastStore.info(wrapper, {
    title: `Connecting ${label} integration`,
  });
};
