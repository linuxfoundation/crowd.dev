-- Tracks the provenance of advisories.cvss so we can distinguish a real
-- vendor-supplied vector from a synthesized fallback derived from the
-- qualitative severity tag. Extensible to 'ghsa' | 'nvd' if we enrich later.
-- Values:
--   'osv_cvss_v3'              numeric score from a CVSS_V3 vector
--   'osv_cvss_v4'              reserved; v4 numeric scoring deferred (see ADR-0001 §CVSS scoring strategy)
--   'osv_qualitative_fallback' synthesized from database_specific.severity tag
--   'osv_malicious_package'    MAL-* id with no CVSS vector (see ADR-0001 §`has_critical_vulnerability` semantics)
ALTER TABLE advisories
    ADD COLUMN cvss_source text;
