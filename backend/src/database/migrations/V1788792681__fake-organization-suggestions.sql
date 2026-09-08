create table "fakeOrganizationSuggestions" (
    "organizationId" uuid primary key not null references organizations (id) on delete cascade,
    "createdAt" timestamp with time zone not null
);
