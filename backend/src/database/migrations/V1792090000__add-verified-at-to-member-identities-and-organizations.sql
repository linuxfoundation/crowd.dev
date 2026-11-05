alter table "memberIdentities" add column if not exists "verifiedAt" timestamptz;
alter table "memberOrganizations" add column if not exists "verifiedAt" timestamptz;
