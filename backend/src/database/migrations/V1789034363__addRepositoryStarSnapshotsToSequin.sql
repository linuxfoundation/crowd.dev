ALTER PUBLICATION sequin_pub ADD TABLE "repositoryStarSnapshots";
ALTER TABLE public."repositoryStarSnapshots" REPLICA IDENTITY DEFAULT;
