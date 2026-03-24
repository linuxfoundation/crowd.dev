-- organization_segments_mv is no longer needed — replaced by organizationSegmentsAgg table (CM-1008)
drop materialized view if exists organization_segments_mv;
