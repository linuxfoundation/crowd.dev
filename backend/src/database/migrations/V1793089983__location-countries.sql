create table public."locationCountries" (
    "location"    text primary key,
    "status"      text not null,
    "countryCode" text,
    "createdAt"   timestamp with time zone not null default now()
);
