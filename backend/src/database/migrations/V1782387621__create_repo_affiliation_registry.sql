create table if not exists git."repoAffiliationRegistry" (
  "repoId"     uuid primary key references public.repositories(id) on delete cascade,
  "filePath"   text,
  "fileSha"    varchar(64),
  "status"     varchar(20) not null,
  "snapshot"   jsonb,
  "lastRunAt"  timestamptz,
  "createdAt"  timestamptz not null default now(),
  "updatedAt"  timestamptz not null default now()
);