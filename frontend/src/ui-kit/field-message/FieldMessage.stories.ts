import { fieldMessageTypes } from '@/ui-kit/field-message/types/FieldMessageType';
import LfFieldMessage from './FieldMessage.vue';
import LfInput from '../input/Input.vue';
import LfField from '../field/Field.vue';

export default {
  title: 'LinuxFoundation/FieldMessage',
  component: LfFieldMessage,
  tags: ['autodocs'],
  argTypes: {
    // Props
    type: {
      description: 'Specifies field message type',
      defaultValue: 'error',
      control: 'select',
      options: fieldMessageTypes,
    },
    hideIcon: {
      description: 'Specifies if message icon is hidden',
      defaultValue: false,
      control: 'boolean',
    },

    // Slots
    default: {
      description: 'Message',
      control: {
        type: null,
      },
    },
    icon: {
      description: 'Icon content',
      control: {
        type: null,
      },
    },
  },
};

const render = (args: Record<string, unknown>) => ({
  components: { LfField, LfInput, LfFieldMessage },
  setup() {
    return { args };
  },
  template: `<lf-field label-text="Field">
      <lf-input model-value="" placeholder="placeholder" :invalid="args.type === 'error'" />
      <lf-field-message :type="args.type" :hide-icon="args.hideIcon">{{args.default}}</lf-field-message>
    </lf-field>`,
});

export const Error = {
  args: {
    type: 'error',
    hideIcon: false,
    default: 'This is error message',
  },
  render,
};

export const Warning = {
  args: {
    type: 'warning',
    hideIcon: false,
    default: 'This is warning',
  },
  render,
};

export const Info = {
  args: {
    type: 'info',
    hideIcon: false,
    default: 'This is info message',
  },
  render,
};

export const Hint = {
  args: {
    type: 'hint',
    hideIcon: false,
    default: 'This is hint for stupid people',
  },
  render,
};
