create table public."mergeSuggestionSources" (
    "type"        text not null,
    "primaryId"   uuid not null,
    "secondaryId" uuid not null,
    "source"      text not null,
    "createdAt"   timestamp with time zone not null default now(),
    primary key ("type", "primaryId", "secondaryId", "source")
);
