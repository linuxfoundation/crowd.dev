import LfField from './Field.vue';
import LfInput from '../input/Input.vue';

export default {
  title: 'LinuxFoundation/Field',
  component: LfField,
  tags: ['autodocs'],
  argTypes: {
    // Props
    labelText: {
      description: 'Specifies label text',
      defaultValue: 'Label',
      control: 'text',
    },
    required: {
      description: 'Specifies if field is required',
      defaultValue: false,
      control: 'boolean',
    },
    for: {
      description: 'for attribute to link with input',
      control: {
        type: null,
      },
    },

    // Slots
    default: {
      description: 'Text or html content of the field',
      control: {
        type: null,
      },
    },
    label: {
      description: 'Label content if needs to be custom',
      control: {
        type: null,
      },
    },
    action: {
      description: 'Action content on right side of field',
      control: {
        type: null,
      },
    },
  },
};

export const Primary = {
  label: 'Required',
  args: {
    labelText: 'Label',
    required: true,
    for: '',
    default: '',
  },
  render: (args: Record<string, unknown>) => ({
    components: { LfField, LfInput },
    setup() {
      return { args };
    },
    template: `<lf-field :label-text="args.labelText" :required="args.required">
      <lf-input model-value="" placeholder="placeholder"></lf-input>
    </lf-field>`,
  }),
};
