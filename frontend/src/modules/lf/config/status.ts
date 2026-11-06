export interface StatusOption {
  class: string;
  color: string;
  label: string;
  value: string;
}

const statusOptions: StatusOption[] = [
  {
    class: 'success',
    color: 'bg-green-500',
    label: 'Active',
    value: 'active',
  },
  {
    class: 'primary',
    color: 'bg-primary-500',
    label: 'Formation',
    value: 'formation',
  },
  {
    class: 'warning',
    color: 'bg-yellow-500',
    label: 'Prospect',
    value: 'prospect',
  },
  {
    class: 'secondary',
    color: 'bg-gray-400',
    label: 'Archived',
    value: 'archived',
  },
];

export default statusOptions;
