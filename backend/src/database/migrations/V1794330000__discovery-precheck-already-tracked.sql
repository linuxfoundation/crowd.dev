-- Rows the discovery pre-check skipped before the kind was written at insert time.
UPDATE public."projectCatalog"
SET "classificationKind" = 'already_tracked'
WHERE action = 'skip'
  AND "classificationKind" IS NULL
  AND "skipReason" = 'repository already tracked in CDP (discovery pre-check)';
