CREATE TABLE public."repositoryStarBackfillFailures" (
    "repositoryId" UUID PRIMARY KEY NOT NULL REFERENCES public.repositories(id) ON DELETE CASCADE,
    "consecutiveFailures" INTEGER NOT NULL DEFAULT 0,
    "lastErrorClass" TEXT,
    "lastErrorMessage" TEXT,
    "deadLetteredAt" TIMESTAMP WITH TIME ZONE,
    "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX "ix_repositoryStarBackfillFailures_deadLetteredAt"
    ON public."repositoryStarBackfillFailures" ("deadLetteredAt")
    WHERE "deadLetteredAt" IS NOT NULL;
