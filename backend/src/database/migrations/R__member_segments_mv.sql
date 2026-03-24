-- member_segments_mv is no longer needed — replaced by memberSegmentsAgg table (CM-1008)
drop materialized view if exists member_segments_mv;