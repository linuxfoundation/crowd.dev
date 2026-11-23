ALTER PUBLICATION sequin_pub ADD TABLE "projectDocReadinessChecks";
ALTER TABLE public."projectDocReadinessChecks" REPLICA IDENTITY FULL;
GRANT SELECT ON "projectDocReadinessChecks" to sequin;
