import { GenericModel } from '@/shared/model/generic-model';
import DateTimeField from '@/shared/fields/date-time-field';
import StringField from '@/shared/fields/string-field';
import OrganizationMemberCountField from '@/modules/organization/organization-member-count-field';
import SearchField from '@/shared/fields/search-field';
import JsonField from '@/shared/fields/json-field';
import StringArrayField from '@/shared/fields/string-array-field';
import IntegerField from '@/shared/fields/integer-field';
import BooleanField from '@/shared/fields/boolean-field';
import GenericField from '@/shared/fields/generic-field';
import OrganizationHeadcountField from './organization-headcount-field';
import OrganizationEmployeesField from './organization-employees-field';
import OrganizationTypeField from './organization-type-field';
import OrganizationPlatformField from './organization-platform-field';

const fields = {
  id: new StringField('id', 'id'),
  name: new StringField('name', 'Name'),
  displayName: new StringField('displayName', 'Name', {
    required: true,
  }),
  description: new StringField(
    'description',
    'Description',
  ),
  location: new StringField('location', 'Location', { filterable: true }),
  createdAt: new DateTimeField(
    'createdAt',
    'createdAt',
  ),
  updatedAt: new DateTimeField(
    'updatedAt',
    'updatedAt',
  ),
  members: new OrganizationMemberCountField(
    'memberCount',
    '# of people',
    { filterable: true },
  ),
  employees: new OrganizationEmployeesField(
    'employees',
    '# of employees',
  ),
  activityCount: new IntegerField(
    'activityCount',
    '# of activities',
    {
      filterable: true,
    },
  ),
  lastActive: new DateTimeField(
    'lastActive',
    'Last activity date',
    {
      filterable: true,
    },
  ),
  revenueRange: new JsonField(
    'revenueRange',
    'Annual Revenue',
  ),
  joinedAt: new DateTimeField('joinedAt', 'Joined date', {
    filterable: true,
  }),
  activeOn: new OrganizationPlatformField(
    'activeOn',
    'Active on',
    { filterable: true },
  ),
  identities: new JsonField(
    'identities',
    'Identities',
  ),
  emails: new StringArrayField('emails', 'E-mail address'),
  phoneNumbers: new StringArrayField(
    'phoneNumbers',
    'Phone number',
  ),

  search: new SearchField('search', 'search', {
    fields: ['name'],
  }),
  headline: new StringField('headline', 'Headline', { filterable: true }),
  size: new OrganizationHeadcountField('size', 'Headcount', { filterable: true }),
  industry: new StringField('industry', 'Industry', { filterable: true }),
  founded: new IntegerField('founded', 'Founded', { filterable: true }),
  type: new OrganizationTypeField('organizationType', 'Type', { filterable: true }),
  profiles: new StringArrayField('profiles', 'Profiles'),
  lastEnrichedAt: new BooleanField('lastEnrichedAt', 'Enriched company', {
    filterable: true,
  }),
  allSubsidiaries: new StringArrayField('allSubsidiaries', 'All Subsidiaries'),
  alternativeNames: new StringArrayField('alternativeNames', 'Alternative Names'),
  averageEmployeeTenure: new GenericField('averageEmployeeTenure', 'Average Employee Tenure'),
  averageTenureByLevel: new JsonField('averageTenureByLevel', 'Average Tenure by Level'),
  averageTenureByRole: new JsonField('averageTenureByRole', 'Average Tenure by Role'),
  directSubsidiaries: new StringArrayField('directSubsidiaries', 'Direct Subsidiaries'),
  employeeChurnRate: new JsonField('employeeChurnRate', 'Employee Churn Rate'),
  employeeCountByCountry: new JsonField('employeeCountByCountry', 'Employee Count by Country'),
  employeeCountByMonth: new JsonField('employeeCountByMonth', 'Employee Count by Month'),
  employeeGrowthRate: new JsonField('employeeGrowthRate', 'Employee Growth Rate'),
  gicsSector: new StringField('gicsSector', 'GICS Sector'),
  grossAdditionsByMonth: new JsonField('grossAdditionsByMonth', 'Gross Additions by Month'),
  grossDeparturesByMonth: new JsonField('grossDeparturesByMonth', 'Gross Departures by Month'),
  immediateParent: new StringField('immediateParent', 'Immediate Parent'),
  tags: new StringArrayField('tags', 'Tags'),
  ultimateParent: new StringField('ultimateParent', 'Ultimate Parent'),
};

export class OrganizationModel extends GenericModel {
  static get fields() {
    return fields;
  }
}
