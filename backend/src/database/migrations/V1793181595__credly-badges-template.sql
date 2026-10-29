ALTER TABLE integration."credlyBadges"
    ADD COLUMN "badgeTemplateName" TEXT NOT NULL DEFAULT '',
    ADD COLUMN "badgeTemplateUrl"  TEXT NULL;

ALTER TABLE integration."credlyBadges"
    ALTER COLUMN "badgeTemplateName" DROP DEFAULT;
