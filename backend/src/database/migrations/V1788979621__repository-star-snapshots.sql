CREATE TABLE public."repositoryStarSnapshots" (
    "id" UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    "repositoryId" UUID NOT NULL REFERENCES public.repositories(id) ON DELETE CASCADE,
    "starCount" INTEGER NOT NULL,
    "capturedAt" TIMESTAMP WITH TIME ZONE NOT NULL,
    "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    UNIQUE ("repositoryId", "capturedAt")
);

CREATE INDEX ix_repositoryStarSnapshots_repositoryId
    ON public."repositoryStarSnapshots" ("repositoryId");

CREATE INDEX ix_repositoryStarSnapshots_updatedAt_id
    ON public."repositoryStarSnapshots" ("updatedAt", "id");
