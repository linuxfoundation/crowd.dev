import { dropdownItemTypes } from '@/ui-kit/dropdown/types/DropdownItemType';
import LfDropdownItem from './DropdownItem.vue';

export default {
  title: 'LinuxFoundation/DropdownItem',
  component: LfDropdownItem,
  tags: ['autodocs'],
  argTypes: {
    type: {
      description: 'Specifies dropdown item type',
      defaultValue: 'regular',
      control: 'select',
      options: dropdownItemTypes,
    },
    disabled: {
      description: 'Specifies dropdown width',
      defaultValue: 'auto',
      control: 'boolean',
    },
  },
};

export const Regular = {
  args: {
    disabled: false,
    type: 'regular',
  },
  render: (args: Record<string, unknown>) => ({
    components: {
      LfDropdownItem,
    },
    setup() {
      return { args };
    },
    template: `<div class="c-dropdown__menu is-open">
      <lf-dropdown-item :disabled="args.disabled" :type="args.type">
        <i class="fa-light fa-link-simple-slash" />
        Dropdown item
      </lf-dropdown-item></div>`,
  }),
};
