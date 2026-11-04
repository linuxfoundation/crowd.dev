import { MemberService } from '@/modules/member/member-service';
import sharedActions from '@/shared/store/actions';

export default {
  ...sharedActions('member', MemberService),
};
