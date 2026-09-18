ALTER PUBLICATION sequin_pub ADD TABLE "projectDocReadiness";
ALTER TABLE public."projectDocReadiness" REPLICA IDENTITY FULL;
GRANT SELECT ON "projectDocReadiness" to sequin;
