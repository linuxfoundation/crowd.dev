import { MemberService } from '@/modules/member/services/member.api.service';
import RelationToOneField from '@/shared/fields/relation-to-one-field';
import RelationToManyField from '@/shared/fields/relation-to-many-field';

export class MemberField {
  static relationToOne(name, label, options) {
    return new RelationToOneField(
      name,
      label,
      '/members',
      null,
      MemberService.listMembersAutocomplete,
      (record) => {
        if (!record) {
          return null;
        }

        return {
          id: record.id,
          label: record.displayName,
        };
      },
      options,
    );
  }

  static relationToMany(name, label, options) {
    return new RelationToManyField(
      name,
      label,
      '/members',
      null,
      MemberService.listMembersAutocomplete,
      (record) => {
        if (!record) {
          return null;
        }

        return {
          id: record.id,
          label: record.displayName,
        };
      },
      options,
    );
  }
}
