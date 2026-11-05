import LfField from '@/ui-kit/field/Field.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import { reactive } from 'vue';
import { email, required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfFieldMessages from './FieldMessages.vue';

export default {
  title: 'LinuxFoundation/FieldMessages',
  component: LfFieldMessages,
  tags: ['autodocs'],
  argTypes: {
    // Props
    validation: {
      description: 'Enter vuelidate validation',
      control: {
        type: null,
      },
    },
    errorMessages: {
      description: 'Specifies custom error messages',
      control: {
        type: 'object',
      },
    },

    // Slots
    icon: {
      description: 'Message icon',
      control: {
        type: null,
      },
    },
  },
};

export const Regular = {
  args: {
    errorMessages: { required: 'This field is required', email: 'Invalid email' },
  },
  render: (args: Record<string, unknown>) => ({
    components: { LfField, LfInput, LfFieldMessages },
    setup() {
      const form = reactive({
        email: '',
      });

      const rules = {
        email: {
          required,
          email,
        },
      };

      const v = useVuelidate(rules, form);

      return { args, form, v };
    },
    template: `<lf-field
        label-text="Email"
        :required="true"
    >
      <lf-input
          v-model="form.email"
          placeholder="Email address"
          :invalid="v.email.$invalid && v.email.$dirty"
          @blur="v.email.$touch()"
      />
      <lf-field-messages
          :validation="v.email"
          :error-messages="args.errorMessages"
      />
    </lf-field>`,
  }),
};
