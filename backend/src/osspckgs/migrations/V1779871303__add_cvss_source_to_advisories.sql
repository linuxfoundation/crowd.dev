-- Tracks the provenance of advisories.cvss so we can distinguish a real
-- vendor-supplied vector from a synthesized fallback derived from the
-- qualitative severity tag. Extensible to 'ghsa' | 'nvd' if we enrich later.
-- Values: 'osv_cvss_v3' | 'osv_qualitative_fallback'
ALTER TABLE advisories
    ADD COLUMN cvss_source text;
