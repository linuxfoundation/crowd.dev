import type { FieldMessageType } from '@/ui-kit/field-message/types/FieldMessageType';

interface FieldMessageTypeData{
  icon: string
}

export const fieldMessageTypeData: Record<FieldMessageType, FieldMessageTypeData> = {
  error: {
    icon: 'circle-exclamation',
  },
  hint: {
    icon: '',
  },
  warning: {
    icon: 'triangle-exclamation',
  },
  info: {
    icon: 'circle-info',
  },
};
