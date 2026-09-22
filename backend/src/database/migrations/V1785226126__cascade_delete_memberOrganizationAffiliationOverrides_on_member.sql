alter table "memberOrganizationAffiliationOverrides"
  drop constraint if exists "memberOrganizationAffiliationOverrides_memberId_fkey";

alter table "memberOrganizationAffiliationOverrides"
  add constraint "memberOrganizationAffiliationOverrides_memberId_fkey"
  foreign key ("memberId")
  references members (id)
  on delete cascade;
