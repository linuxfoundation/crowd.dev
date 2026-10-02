import { h } from 'vue';
import { ToastStore } from '@/shared/message/notification';
import type { SuccessMessage } from '../../types/MemberMessage';

export default ({ primaryMember, secondaryMember, selectedProjectGroupId }: SuccessMessage) => {
  const { id, displayName: primaryDisplayName } = primaryMember;
  const { displayName: secondaryDisplayName } = secondaryMember;

  ToastStore.closeAll();
  ToastStore.success(
    h(
      'div',
      {
        class: 'flex flex-col gap-2',
      },
      [
        h(
          'span',
          {
            innerHTML: `${secondaryDisplayName} merged with ${primaryDisplayName}.`,
          },
        ),
        h(
          'button',
          {
            class: 'c-btn c-btn--tiny c-btn--secondary-gray !h-6 !w-fit',
            onClick: async () => {
              const { router } = await import('@/router');
              router.push({
                name: 'memberView',
                params: { id },
                query: { projectGroup: selectedProjectGroupId },
              });
              ToastStore.closeAll();
            },
          },
          'View profile',
        ),
      ],
    ),
    {
      title:
        'Profiles merged successfully',
    },
  );
};
