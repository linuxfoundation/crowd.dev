-- Lines the bot adds under the repositories, such as why an organization link was not expanded.
ALTER TABLE public."discussionOnboardingRequests"
  ADD COLUMN "notes" JSONB NOT NULL DEFAULT '[]'::jsonb;
