import type { UserState } from '@/modules/user/store/pinia/state';

export default {
  updateDeveloperMode(this: UserState, value: boolean): void {
    this.isDeveloperModeActive = value;
    localStorage.setItem('developerMode', JSON.stringify(value));
  },
};
