import ConfirmDialog from '@/shared/dialog/confirm-dialog';

export const showExportDialog = ({ badgeContent }: { badgeContent: string }) =>
  ConfirmDialog({
    vertical: true,
    type: 'info',
    title: 'Export CSV',
    message: 'Receive in your inbox a link to download the CSV file ',
    icon: 'fa-file-arrow-down fa-light',
    confirmButtonText: 'Send download link to e-mail',
    cancelButtonText: 'Cancel',
    badgeContent,
    highlightedInfo: undefined,
  });
