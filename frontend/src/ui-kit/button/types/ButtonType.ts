export const buttonTypes = [
  'primary',
  'danger',
  'secondary',
  'secondary-gray',
  'bordered',
  'primary-ghost',
  'secondary-ghost',
  'danger-ghost',
  'secondary-ghost-light',
  'primary-link',
  'secondary-link',
  'outline',
] as const;

export type ButtonType = (typeof buttonTypes)[number]
