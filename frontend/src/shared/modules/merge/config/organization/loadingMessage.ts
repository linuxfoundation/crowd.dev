import { ToastStore } from '@/shared/message/notification';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import { storeToRefs } from 'pinia';

export default () => {
  const organizationStore = useOrganizationStore();
  const {
    mergedOrganizations,
  } = storeToRefs(organizationStore);

  const processesRunning = Object.keys(mergedOrganizations.value).length;

  ToastStore.closeAll();
  ToastStore.info('', {
    title: 'Companies merging in progress',
    message: processesRunning > 1 ? `${processesRunning} processes running` : undefined,
  });
};
