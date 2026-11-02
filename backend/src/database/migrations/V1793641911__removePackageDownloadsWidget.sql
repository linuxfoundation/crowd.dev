-- Remove the disabled packageDownloads widget from all projects

UPDATE "insightsProjects"
SET
    widgets = array_remove(widgets, 'packageDownloads'),
    "updatedAt" = CURRENT_TIMESTAMP
WHERE 'packageDownloads' = ANY(widgets);
