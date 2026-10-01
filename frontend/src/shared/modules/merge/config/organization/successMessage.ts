import { h } from 'vue';
import { ToastStore } from '@/shared/message/notification';
import type { SuccessMessage } from '../../types/OrganizationMessage';

export default ({ primaryOrganization, secondaryOrganization }: SuccessMessage) => {
  const { id, displayName: primaryDisplayName } = primaryOrganization;
  const { displayName: secondaryDisplayName } = secondaryOrganization;

  const buttonElement = h(
    'button',
    {
      class: 'c-btn c-btn--tiny c-btn--secondary-gray !h-6 !w-fit',
      onClick: async () => {
        const { router } = await import('@/router');
        router.push({
          name: 'organizationView',
          params: { id },
        });
        ToastStore.closeAll();
      },
    },
    'View organization',
  );

  const messageElements = [buttonElement];

  if (primaryDisplayName && secondaryDisplayName) {
    const descriptionElement = h(
      'span',
      {
        innerHTML: `${secondaryDisplayName} merged with ${primaryDisplayName}.`,
      },
    );

    messageElements.unshift(descriptionElement);
  }

  ToastStore.closeAll();
  ToastStore.success(
    h(
      'div',
      {
        class: 'flex flex-col gap-2',
      },
      messageElements,
    ),
    {
      title: 'Organizations merged successfully',
    },
  );
};
