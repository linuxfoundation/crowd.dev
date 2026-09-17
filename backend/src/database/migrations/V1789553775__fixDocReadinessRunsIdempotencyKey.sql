-- workflowId alone conflates distinct Temporal executions on retry; key on temporalRunId instead.
DROP INDEX public."ux_projectDocReadinessRuns_workflowId";

CREATE UNIQUE INDEX "ux_projectDocReadinessRuns_temporalRunId"
    ON public."projectDocReadinessRuns" ("temporalRunId")
    WHERE "temporalRunId" IS NOT NULL;
