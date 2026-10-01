import type { Member } from '@/modules/member/types/Member';
import type { AxiosError } from 'axios';

export interface SuccessMessage {
    primaryMember: Member;
    secondaryMember: Member;
    selectedProjectGroupId: number;
}

export interface ErrorMessage {
    error: AxiosError
}

export interface MemberMessage {
    loadingMessage: () => void;
    successMessage: ({ primaryMember, secondaryMember, selectedProjectGroupId }: SuccessMessage) => void;
    apiErrorMessage: ({ error }: ErrorMessage) => void;
  }
