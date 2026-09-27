import { ElMessageBox } from 'element-plus';
import type { MessageBoxData } from 'element-plus';
import { h } from 'vue';
import 'element-plus/dist/index.css';

export interface ConfirmDialogOptions {
  vertical?: boolean;
  type?: 'warning' | 'danger' | 'info' | 'success' | 'notification' | 'custom' | 'error';
  title?: string;
  message?: string;
  badgeContent?: string;
  highlightedInfo?: string;
  showCancelButton?: boolean;
  showClose?: boolean;
  customClass?: string;
  cancelButtonText?: string;
  cancelButtonClass?: string;
  confirmButtonText?: string;
  confirmButtonClass?: string;
  icon?: string;
  distinguishCancelAndClose?: boolean;
  autofocus?: boolean;
  closeOnClickModal?: boolean;
  titleClass?: string | null;
  messageClass?: string | null;
  verticalCancelButtonClass?: string | null;
  verticalConfirmButtonClass?: string | null;
  verticalCustomClass?: string | null;
  hideCloseButton?: boolean;
}

export default ({
  vertical = false,
  type = 'warning',
  title = 'Unsaved changes',
  message = 'Are you sure you want to leave this page? Unsaved changes will be lost',
  badgeContent = undefined,
  highlightedInfo = undefined,
  showCancelButton = true,
  showClose = false,
  customClass = 'confirm-dialog',
  cancelButtonText = 'Stay on this page',
  cancelButtonClass = 'btn btn--md btn--secondary',
  confirmButtonText = 'Discard',
  confirmButtonClass = 'btn btn--md btn--primary',
  icon = 'fa-circle-exclamation fa-light',
  distinguishCancelAndClose = false,
  autofocus = true,
  closeOnClickModal = true,
  titleClass = null,
  messageClass = null,
  verticalCancelButtonClass = null,
  verticalConfirmButtonClass = null,
  verticalCustomClass = null,
  hideCloseButton = false,
}: ConfirmDialogOptions): Promise<MessageBoxData> => {
  let iconColorClass = 'text-yellow-600';
  let iconBgColorClass = 'bg-yellow-100';

  if (type === 'danger') {
    iconColorClass = 'text-red-600';
    iconBgColorClass = 'bg-red-100';
  } else if (type === 'info') {
    iconColorClass = 'text-gray-500';
    iconBgColorClass = 'bg-gray-100';
  } else if (type === 'success') {
    iconColorClass = 'text-green-500';
    iconBgColorClass = 'bg-green-100';
  } else if (type === 'notification') {
    iconColorClass = 'text-primary-600';
    iconBgColorClass = 'bg-primary-50';
  }

  let content = h(
    'div', // type
    {
      class: 'flex',
    }, // props
    [
      type === 'custom' ? h(
        'span',
        {
          innerHTML: icon,
          class: '',
        },
      )
        : h(
          'span', // type
          {
            class: `rounded-full ${iconBgColorClass} w-10 h-10 flex items-center justify-center absolute custom-icon`,
          }, // props
          [
            h(
              'i', // type
              {
                class: `${icon} text-lg ${iconColorClass} leading-none`,
              }, // props
              [],
            ),
          ],
        ),
      h('div', [
        h('p', {
          innerHTML: message,
          class: `text-gray-500 text-sm ${messageClass}`,
        }),
        highlightedInfo
          ? h(
            'div',
            {
              class:
                  'text-2xs text-yellow-600 flex items-center mt-4',
            },
            [
              h('div', {
                innerHTML: highlightedInfo,
              }),
            ],
          )
          : undefined,
      ]),
    ],
  );

  if (vertical) {
    content = h(
      'div', // type
      {}, // props
      [
        h(
          'div',
          {
            class: 'flex justify-between items-center mb-4',
          },
          [
            type === 'custom' ? h(
              'span',
              {
                innerHTML: icon,
                class: '',
              },
            )
              : h(
                'span', // type
                {
                  class: `rounded-full ${iconBgColorClass} w-10 h-10 flex items-center justify-center custom-icon`,
                }, // props
                [
                  h(
                    'i', // type
                    {
                      class: `${icon} text-lg ${iconColorClass} leading-none`,
                    }, // props
                    [],
                  ),
                ],
              ),
            !hideCloseButton ? h(
              'button',
              {
                class:
                  'c-btn c-btn--tiny c-btn--primary-link w-8 !h-8 hover:!no-underline group',
                type: 'button',
                onClick: () => {
                  document
                    .querySelector(
                      '.el-message-box__headerbtn',
                    )
                    ?.dispatchEvent(new Event('click'));
                },
              },
              [
                h(
                  'i', // type
                  {
                    class: 'text-lg fa-xmark fa-light leading-none text-gray-400 group-hover:text-primary-500',
                  }, // props
                  [],
                ),
              ],
            ) : null,
          ],
        ),
        h('h6', {
          innerHTML: title,
          class: `text-black mb-3 ${titleClass}`,
        }),
        badgeContent
          ? h('div', {
            class:
                'rounded-lg border border-gray-300 px-2 mb-3 inline-flex text-xs text-gray-900 h-6 items-center',
            innerHTML: badgeContent,
          })
          : undefined,
        h('p', {
          innerHTML: message,
          class: `text-gray-500 text-sm ${messageClass}`,
        }),
        highlightedInfo
          ? h(
            'div',
            {
              class:
                  'text-2xs text-yellow-600 flex items-center mt-4',
            },
            [
              h(
                'i', // type
                {
                  class: 'text-base fa-triangle-exclamation fa-light leading-none mr-2',
                }, // props
                [],
              ),
              h('div', {
                innerHTML: highlightedInfo,
              }),
            ],
          )
          : undefined,
      ],
    );

    const overrideCustomClass = `confirm-dialog confirm-dialog--vertical ${verticalCustomClass}`;
    const overrideConfirmButtonClass = 'btn btn--md btn--primary w-full';
    const overrideCancelButtonClass = 'btn btn-link btn-link--md btn-link--primary w-full';

    return ElMessageBox({
      title: '',
      message: content,
      showClose: true,
      showCancelButton,
      customClass: overrideCustomClass,
      confirmButtonText,
      confirmButtonClass: verticalConfirmButtonClass || overrideConfirmButtonClass,
      cancelButtonText,
      cancelButtonClass: verticalCancelButtonClass || overrideCancelButtonClass,
      distinguishCancelAndClose,
      autofocus,
      closeOnClickModal,
    });
  }

  return ElMessageBox({
    title,
    message: content,
    showCancelButton,
    showClose,
    customClass,
    cancelButtonText,
    cancelButtonClass,
    confirmButtonText,
    confirmButtonClass,
    distinguishCancelAndClose,
    autofocus,
    closeOnClickModal,
  });
};
