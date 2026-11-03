UPDATE "insightsProjects"
SET
    widgets = array_remove(widgets, 'packageDependency'),
    "updatedAt" = CURRENT_TIMESTAMP
WHERE 'packageDependency' = ANY(widgets);
