-- One row per GitHub discussion request: the comment the bot edits in place as the repos resolve.
CREATE TABLE public."discussionOnboardingRequests" (
    "sourceUrl"      TEXT PRIMARY KEY,
    "discussionId"   TEXT NOT NULL,
    "commentId"      TEXT NOT NULL,
    "doneNotifiedAt" TIMESTAMP WITH TIME ZONE,
    "createdAt"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
