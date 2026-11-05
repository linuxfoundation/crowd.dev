import LfButton from '@/ui-kit/button/Button.vue';
import LfPopover from '@/ui-kit/popover/Popover.vue';
import { popoverPlacements } from '@/ui-kit/popover/types/PopoverPlacement';
import { popoverTrigger } from '@/ui-kit/popover/types/PopoverTrigger';

export default {
  title: 'LinuxFoundation/Popover',
  component: LfPopover,
  tags: ['autodocs'],
  argTypes: {
    // Props
    placement: {
      description: 'Popover placement',
      defaultValue: 'bottom-start',
      control: 'select',
      options: popoverPlacements,
    },
    triggerEvent: {
      description: 'Popover trigger',
      defaultValue: 'click',
      control: 'select',
      options: popoverTrigger,
    },
    spacing: {
      description: 'Popover spacing',
      defaultValue: '4',
      control: 'number',
    },
    disabled: {
      description: 'Specifies if popover is hidden',
      defaultValue: false,
      control: 'boolean',
    },

    // Slots
    default: {
      description: 'Any content belonging to popover',
      control: {
        type: null,
      },
    },
    trigger: {
      description: 'Any content belonging to trigger',
      control: {
        type: null,
      },
    },
  },
};

export const Default = {
  args: {
    placement: 'bottom-start',
    disabled: false,
    spacing: 4,
    triggerEvent: 'click',
  },
  render: (args: Record<string, unknown>) => ({
    components: { LfPopover, LfButton },
    setup() {
      return { args };
    },
    template: `<lf-popover :spacing="args.spacing" :disabled="args.disabled" :placement="args.placement" :trigger-event="args.triggerEvent">
      <template #trigger>
        <lf-button>Open Popover</lf-button>
      </template>
      <div class="p-10 bg-white border rounded-l border-gray-100">
        <p>This is popover content</p>
      </div>
    </lf-popover>`,
  }),
};
