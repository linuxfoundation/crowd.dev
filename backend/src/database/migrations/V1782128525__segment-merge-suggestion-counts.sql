create table "segmentMergeSuggestionCounts" (
  "segmentId" uuid primary key references "segments" ("id") on delete cascade,
  "memberMergeSuggestionsCount" integer not null default 0,
  "organizationMergeSuggestionsCount" integer not null default 0,
  "updatedAt" timestamp with time zone
);