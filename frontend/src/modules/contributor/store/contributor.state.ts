import type { Contributor } from '@/modules/contributor/types/Contributor';

export interface ContributorState {
  contributor: Contributor | null;
  reportDataModal: any | null;
}

export default () => ({
  contributor: null,
  reportDataModal: null,
} as ContributorState);
