import { ref } from 'vue';
import { tabsSize } from '@/ui-kit/tabs/types/TabsSize';
import LfTabs from './Tabs.vue';
import LfTab from './Tab.vue';

export default {
  title: 'LinuxFoundation/Tabs',
  component: LfTabs,
  tags: ['autodocs'],
  argTypes: {
    // Props
    size: {
      description: 'Specifies badge size',
      defaultValue: 'medium',
      control: 'select',
      options: tabsSize,
    },

    modelValue: {
      description: 'Selected tabs value',
      control: {
        type: null,
      },
    },

    // Slots
    default: {
      description: '<lf-tab> components',
      control: {
        type: null,
      },
    },

    // Events
    'update:modelValue': {
      description: 'Event triggered when tabs value changes',
      control: {
        type: null,
      },
    },
  },
};

export const Regular = {
  args: {
    size: 'medium',
  },
  render: (args: Record<string, unknown>) => ({
    components: { LfTabs, LfTab },
    setup() {
      const activeTab = ref('primary');
      return { activeTab, args };
    },
    template: `<lf-tabs v-model="activeTab" :size="args.size">
          <lf-tab name="primary">Primary</lf-tab>
          <lf-tab name="secondary">Secondary</lf-tab>
          <lf-tab name="tertiary">Tertiary</lf-tab>
    </lf-tabs>`,
  }),
};
