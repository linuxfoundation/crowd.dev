ALTER TABLE "llmSuggestionVerdicts" ADD COLUMN "response" JSONB;

UPDATE "llmSuggestionVerdicts"
SET "response" = jsonb_build_object(
  'decision', CASE
    WHEN lower("verdict") ~ '\mtrue[\s.*''"`]*$' THEN true
    WHEN lower("verdict") ~ '\mfalse[\s.*''"`]*$' THEN false
  END,
  'reason', CASE WHEN "verdict" IN ('true', 'false') THEN NULL ELSE "verdict" END
);

ALTER TABLE "llmSuggestionVerdicts"
  ALTER COLUMN "response" SET NOT NULL,
  ADD CONSTRAINT "llmSuggestionVerdicts_response_decision_check"
    CHECK (jsonb_typeof("response" -> 'decision') = 'boolean'),
  DROP COLUMN "verdict";
