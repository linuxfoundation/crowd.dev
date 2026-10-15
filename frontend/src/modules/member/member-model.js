import { GenericModel } from '@/shared/model/generic-model';
import DateTimeField from '@/shared/fields/date-time-field';
import StringField from '@/shared/fields/string-field';
import JsonField from '@/shared/fields/json-field';
import { OrganizationField } from '@/modules/organization/organization-field';
import StringArrayField from '@/shared/fields/string-array-field';
import GenericField from '@/shared/fields/generic-field';
import { dateHelper } from '@/shared/date-helper/date-helper';
import MemberEngagementLevelField from './member-engagement-level-field';

const fields = {
  jobTitle: new StringField('jobTitle', 'Job title'),
  username: new JsonField('username', 'Username', {
    nonEmpty: true,
    nonEmptyValues: true,
    requiredUnless: 'email',
    customFilterPreview: (record) => record,
  }),
  attributes: new JsonField(
    'attributes',
    'attributes',
  ),
  identities: new JsonField(
    'identities',
    'Identities',
  ),
  name: new StringField(
    'name',
    'Name',
  ),
  displayName: new StringField(
    'displayName',
    'Full name',
  ),
  info: new JsonField('info', 'Custom Attributes'),
  emails: new StringArrayField('emails', 'Emails'),
  organizations: OrganizationField.relationToMany(
    'organizations',
    'Organizations',
  ),
  joinedAt: new DateTimeField('joinedAt', 'Joined date', {
    filterable: true,
    formatter: (value) => {
      if (!value || new Date(value).getFullYear() <= 1970) {
        return '-';
      }
      return dateHelper(value).format('YYYY-MM-DD');
    },
  }),
  bio: new StringField('bio', 'Bio'),
  location: new StringField(
    'location',
    'Location',
    {},
  ),
  engagementLevel: new MemberEngagementLevelField(
    'score',
    'Engagement level',
  ),
  affiliations: new GenericField('affiliations', 'Affiliations'),
};

export class MemberModel extends GenericModel {
  static get fields() {
    return fields;
  }
}
