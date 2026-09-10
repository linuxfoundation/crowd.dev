alter table "memberOrganizations"
    add column if not exists "deletedBy" varchar(255) default null;
