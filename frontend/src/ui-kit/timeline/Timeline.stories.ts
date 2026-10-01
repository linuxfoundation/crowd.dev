import LfSvg from '@/shared/svg/svg.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTimeline from './Timeline.vue';
import LfTimelineItem from './TimelineItem.vue';
import type { TimelineGroup } from './types/TimelineTypes';

export default {
  title: 'LinuxFoundation/Timeline',
  component: LfTimeline,
  tags: ['autodocs'],
  argTypes: {
    groups: {
      description: 'Timeline groups',
      control: 'object',
    },
  },
};

export const Regular = {
  args: {
    groups: [
      {
        label: 'Group 1', labelLink: { name: 'organizationView', params: { id: '1' } }, icon: 'https://avatars.githubusercontent.com/u/38015056?v=4', items: [{ id: 1, label: 'Item 1', date: 'Aug 2024 - Dec 2024' }, { id: 2, label: 'Item 2', date: 'Aug 2024' }, { id: 3, label: 'Item 3', date: 'Aug 2024' }, { id: 4, label: 'Item 4', date: 'Aug 2024' }],
      },
      {
        label: 'Group 2', labelLink: { name: 'organizationView', params: { id: '2' } }, icon: 'https://avatars.githubusercontent.com/u/38015056?v=4', items: [{ id: 3, label: 'Item 3', date: 'Aug 2024' }, { id: 4, label: 'Item 4', date: 'Aug 2024' }],
      },
    ],
  },
  render: (args: { groups: TimelineGroup[] }) => ({
    components: {
      LfTimeline, LfTimelineItem, LfSvg, LfIcon,
    },
    setup() {
      return { args };
    },
    template: `<lf-timeline :groups="args.groups" v-slot="{ group }">
      <lf-timeline-item :data="item" v-for="item in group.items" :key="item.id">
        <!-- SAMPLE CONTENT -->
        <div class="text-small text-gray-900 mb-1.5 flex items-center gap-1.5">
          <lf-svg name="id-card" class="h-4 w-4 text-gray-400" />
          <p class="truncate text-gray-900" style="max-width: 30ch">
            {{ item.label }}
          </p>
        </div>
        <p class="text-small text-gray-500 mb-1.5 flex items-center">
        <lf-icon name="calendar fa-sharp" :size="16" class="mr-1.5 text-gray-400" />
          {{ item.date }}
        </p>
      </lf-timeline-item>
    </lf-timeline>`,
  }),
};
