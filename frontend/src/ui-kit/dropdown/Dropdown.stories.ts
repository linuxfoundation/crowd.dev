import { ref } from 'vue';
import { dropdownPlacements } from '@/ui-kit/dropdown/types/DropdownPlacement';
import LfDropdown from './Dropdown.vue';
import LfDropdownItem from './DropdownItem.vue';
import LfDropdownSeparator from './DropdownSeparator.vue';
import LfButton from '../button/Button.vue';

export default {
  title: 'LinuxFoundation/Dropdown',
  component: LfDropdown,
  tags: ['autodocs'],
  argTypes: {
    placement: {
      description: 'Specifies dropdown placement',
      defaultValue: 'bottom-start',
      control: 'select',
      options: dropdownPlacements,
    },
    width: {
      description: 'Specifies dropdown width',
      defaultValue: 'auto',
      control: 'text',
    },
  },
};

export const Regular = {
  args: {
    placement: 'bottom-start',
    width: 'auto',
  },
  render: (args: Record<string, unknown>) => ({
    components: {
      LfDropdown,
      LfDropdownItem,
      LfButton,
      LfDropdownSeparator,
    },
    setup() {
      return { args };
    },
    template: `<div class="p-20"><lf-dropdown :placement="args.placement" :width="args.width">
      <template #trigger>
        <lf-button>Open dropdown</lf-button>
      </template>
      <lf-dropdown-item :disabled="true">
        <i class="fa-light fa-link-simple-slash" />
        Unmerge identity
      </lf-dropdown-item>
      <lf-dropdown-item>
        <i class="fa-light fa-badge-check" />
        Verify identity
      </lf-dropdown-item>
      <lf-dropdown-separator />
      <lf-dropdown-item type="danger">
        <i class="fa-light fa-trash-can" />
        Delete identity
      </lf-dropdown-item>
    </lf-dropdown></div>`,
  }),
};

export const Selected = {
  args: {
  },
  render: () => ({
    components: { LfDropdown, LfDropdownItem, LfButton },
    setup() {
      const selected = ref(1);

      return { selected };
    },
    template: `<lf-dropdown>
      <template #trigger>
        <lf-button>Open dropdown</lf-button>
      </template>
      <lf-dropdown-item @click="selected = 1" :selected="selected === 1">First item</lf-dropdown-item>
      <lf-dropdown-item @click="selected = 2" :selected="selected === 2">Second item</lf-dropdown-item>
      <lf-dropdown-item @click="selected = 3" :selected="selected === 3">Third item</lf-dropdown-item>
    </lf-dropdown>`,
  }),
};
