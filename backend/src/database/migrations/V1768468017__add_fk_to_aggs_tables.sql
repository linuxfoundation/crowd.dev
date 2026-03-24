ALTER TABLE public."memberSegmentsAgg"
ADD CONSTRAINT "memberSegmentsAgg_memberId_fkey"
FOREIGN KEY ("memberId")
REFERENCES public."members"(id)
ON DELETE CASCADE;

ALTER TABLE public."organizationSegmentsAgg"
ADD CONSTRAINT "organizationSegmentsAgg_organizationId_fkey"
FOREIGN KEY ("organizationId")
REFERENCES public."organizations"(id)
ON DELETE CASCADE;
