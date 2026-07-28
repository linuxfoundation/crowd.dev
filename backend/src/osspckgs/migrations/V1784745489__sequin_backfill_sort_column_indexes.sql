CREATE INDEX IF NOT EXISTS versions_last_synced_at_id_package_id_idx
  ON ONLY versions (last_synced_at, id, package_id);

DO $$
DECLARE idx text;
BEGIN
  FOR idx IN
    SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
    WHERE NOT i.indisvalid AND c.relname ~ '^versions_p[0-9]+_last_synced_at_id_package_id_idx$'
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS %I', idx);
  END LOOP;
END$$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p0_last_synced_at_id_package_id_idx
  ON versions_p0 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p1_last_synced_at_id_package_id_idx
  ON versions_p1 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p2_last_synced_at_id_package_id_idx
  ON versions_p2 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p3_last_synced_at_id_package_id_idx
  ON versions_p3 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p4_last_synced_at_id_package_id_idx
  ON versions_p4 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p5_last_synced_at_id_package_id_idx
  ON versions_p5 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p6_last_synced_at_id_package_id_idx
  ON versions_p6 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p7_last_synced_at_id_package_id_idx
  ON versions_p7 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p8_last_synced_at_id_package_id_idx
  ON versions_p8 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p9_last_synced_at_id_package_id_idx
  ON versions_p9 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p10_last_synced_at_id_package_id_idx
  ON versions_p10 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p11_last_synced_at_id_package_id_idx
  ON versions_p11 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p12_last_synced_at_id_package_id_idx
  ON versions_p12 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p13_last_synced_at_id_package_id_idx
  ON versions_p13 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p14_last_synced_at_id_package_id_idx
  ON versions_p14 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p15_last_synced_at_id_package_id_idx
  ON versions_p15 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p16_last_synced_at_id_package_id_idx
  ON versions_p16 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p17_last_synced_at_id_package_id_idx
  ON versions_p17 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p18_last_synced_at_id_package_id_idx
  ON versions_p18 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p19_last_synced_at_id_package_id_idx
  ON versions_p19 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p20_last_synced_at_id_package_id_idx
  ON versions_p20 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p21_last_synced_at_id_package_id_idx
  ON versions_p21 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p22_last_synced_at_id_package_id_idx
  ON versions_p22 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p23_last_synced_at_id_package_id_idx
  ON versions_p23 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p24_last_synced_at_id_package_id_idx
  ON versions_p24 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p25_last_synced_at_id_package_id_idx
  ON versions_p25 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p26_last_synced_at_id_package_id_idx
  ON versions_p26 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p27_last_synced_at_id_package_id_idx
  ON versions_p27 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p28_last_synced_at_id_package_id_idx
  ON versions_p28 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p29_last_synced_at_id_package_id_idx
  ON versions_p29 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p30_last_synced_at_id_package_id_idx
  ON versions_p30 (last_synced_at, id, package_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_p31_last_synced_at_id_package_id_idx
  ON versions_p31 (last_synced_at, id, package_id);

DO $$
DECLARE part text;
BEGIN
  FOR part IN
    SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = 'versions'::regclass
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_inherits
      WHERE inhrelid = (part || '_last_synced_at_id_package_id_idx')::regclass
    ) THEN
      EXECUTE format('ALTER INDEX versions_last_synced_at_id_package_id_idx ATTACH PARTITION %I', part || '_last_synced_at_id_package_id_idx');
    END IF;
  END LOOP;
END$$;

CREATE INDEX IF NOT EXISTS package_dependencies_updated_at_id_depends_on_id_idx
  ON ONLY package_dependencies (updated_at, id, depends_on_id);

DO $$
DECLARE idx text;
BEGIN
  FOR idx IN
    SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
    WHERE NOT i.indisvalid AND c.relname ~ '^package_dependencies_p[0-9]+_updated_at_id_depends_on_id_idx$'
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS %I', idx);
  END LOOP;
END$$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p0_updated_at_id_depends_on_id_idx
  ON package_dependencies_p0 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p1_updated_at_id_depends_on_id_idx
  ON package_dependencies_p1 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p2_updated_at_id_depends_on_id_idx
  ON package_dependencies_p2 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p3_updated_at_id_depends_on_id_idx
  ON package_dependencies_p3 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p4_updated_at_id_depends_on_id_idx
  ON package_dependencies_p4 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p5_updated_at_id_depends_on_id_idx
  ON package_dependencies_p5 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p6_updated_at_id_depends_on_id_idx
  ON package_dependencies_p6 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p7_updated_at_id_depends_on_id_idx
  ON package_dependencies_p7 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p8_updated_at_id_depends_on_id_idx
  ON package_dependencies_p8 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p9_updated_at_id_depends_on_id_idx
  ON package_dependencies_p9 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p10_updated_at_id_depends_on_id_idx
  ON package_dependencies_p10 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p11_updated_at_id_depends_on_id_idx
  ON package_dependencies_p11 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p12_updated_at_id_depends_on_id_idx
  ON package_dependencies_p12 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p13_updated_at_id_depends_on_id_idx
  ON package_dependencies_p13 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p14_updated_at_id_depends_on_id_idx
  ON package_dependencies_p14 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p15_updated_at_id_depends_on_id_idx
  ON package_dependencies_p15 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p16_updated_at_id_depends_on_id_idx
  ON package_dependencies_p16 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p17_updated_at_id_depends_on_id_idx
  ON package_dependencies_p17 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p18_updated_at_id_depends_on_id_idx
  ON package_dependencies_p18 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p19_updated_at_id_depends_on_id_idx
  ON package_dependencies_p19 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p20_updated_at_id_depends_on_id_idx
  ON package_dependencies_p20 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p21_updated_at_id_depends_on_id_idx
  ON package_dependencies_p21 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p22_updated_at_id_depends_on_id_idx
  ON package_dependencies_p22 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p23_updated_at_id_depends_on_id_idx
  ON package_dependencies_p23 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p24_updated_at_id_depends_on_id_idx
  ON package_dependencies_p24 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p25_updated_at_id_depends_on_id_idx
  ON package_dependencies_p25 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p26_updated_at_id_depends_on_id_idx
  ON package_dependencies_p26 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p27_updated_at_id_depends_on_id_idx
  ON package_dependencies_p27 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p28_updated_at_id_depends_on_id_idx
  ON package_dependencies_p28 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p29_updated_at_id_depends_on_id_idx
  ON package_dependencies_p29 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p30_updated_at_id_depends_on_id_idx
  ON package_dependencies_p30 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p31_updated_at_id_depends_on_id_idx
  ON package_dependencies_p31 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p32_updated_at_id_depends_on_id_idx
  ON package_dependencies_p32 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p33_updated_at_id_depends_on_id_idx
  ON package_dependencies_p33 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p34_updated_at_id_depends_on_id_idx
  ON package_dependencies_p34 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p35_updated_at_id_depends_on_id_idx
  ON package_dependencies_p35 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p36_updated_at_id_depends_on_id_idx
  ON package_dependencies_p36 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p37_updated_at_id_depends_on_id_idx
  ON package_dependencies_p37 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p38_updated_at_id_depends_on_id_idx
  ON package_dependencies_p38 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p39_updated_at_id_depends_on_id_idx
  ON package_dependencies_p39 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p40_updated_at_id_depends_on_id_idx
  ON package_dependencies_p40 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p41_updated_at_id_depends_on_id_idx
  ON package_dependencies_p41 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p42_updated_at_id_depends_on_id_idx
  ON package_dependencies_p42 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p43_updated_at_id_depends_on_id_idx
  ON package_dependencies_p43 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p44_updated_at_id_depends_on_id_idx
  ON package_dependencies_p44 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p45_updated_at_id_depends_on_id_idx
  ON package_dependencies_p45 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p46_updated_at_id_depends_on_id_idx
  ON package_dependencies_p46 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p47_updated_at_id_depends_on_id_idx
  ON package_dependencies_p47 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p48_updated_at_id_depends_on_id_idx
  ON package_dependencies_p48 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p49_updated_at_id_depends_on_id_idx
  ON package_dependencies_p49 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p50_updated_at_id_depends_on_id_idx
  ON package_dependencies_p50 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p51_updated_at_id_depends_on_id_idx
  ON package_dependencies_p51 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p52_updated_at_id_depends_on_id_idx
  ON package_dependencies_p52 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p53_updated_at_id_depends_on_id_idx
  ON package_dependencies_p53 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p54_updated_at_id_depends_on_id_idx
  ON package_dependencies_p54 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p55_updated_at_id_depends_on_id_idx
  ON package_dependencies_p55 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p56_updated_at_id_depends_on_id_idx
  ON package_dependencies_p56 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p57_updated_at_id_depends_on_id_idx
  ON package_dependencies_p57 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p58_updated_at_id_depends_on_id_idx
  ON package_dependencies_p58 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p59_updated_at_id_depends_on_id_idx
  ON package_dependencies_p59 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p60_updated_at_id_depends_on_id_idx
  ON package_dependencies_p60 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p61_updated_at_id_depends_on_id_idx
  ON package_dependencies_p61 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p62_updated_at_id_depends_on_id_idx
  ON package_dependencies_p62 (updated_at, id, depends_on_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_p63_updated_at_id_depends_on_id_idx
  ON package_dependencies_p63 (updated_at, id, depends_on_id);

DO $$
DECLARE part text;
BEGIN
  FOR part IN
    SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = 'package_dependencies'::regclass
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_inherits
      WHERE inhrelid = (part || '_updated_at_id_depends_on_id_idx')::regclass
    ) THEN
      EXECUTE format('ALTER INDEX package_dependencies_updated_at_id_depends_on_id_idx ATTACH PARTITION %I', part || '_updated_at_id_depends_on_id_idx');
    END IF;
  END LOOP;
END$$;
