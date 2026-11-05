create table public."githubEmailLookups" (
    email text primary key,
    result text not null,
    "githubId" text,
    "githubLogin" text,
    "commitSha" text not null,
    "createdAt" timestamp with time zone not null default now()
);
