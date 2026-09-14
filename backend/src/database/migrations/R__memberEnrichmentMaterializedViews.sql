-- global member activity counts
drop materialized view if exists "membersGlobalActivityCount" cascade;
create materialized view "membersGlobalActivityCount" as
select msa."memberId",
    sum(msa."activityCount") AS total_count
from "memberSegmentsAgg" msa
where msa."segmentId" IN (
        select id
        from segments
        where "grandparentId" is not null
            and "parentId" is not null
    )
group by msa."memberId";
create unique index ix_member_global_activity_count_member_id on "membersGlobalActivityCount" ("memberId");
create index ix_member_global_activity_count on "membersGlobalActivityCount" (total_count);

-- member enrichment monitoring (total)
drop materialized view if exists "memberEnrichmentMonitoringTotal";
create materialized view "memberEnrichmentMonitoringTotal" as
with all_members as (
    select count(*) as count from members
),
total_enrichable_members as (
    with enrichable_in_at_least_one_source as (
        select mem.id
        from members mem
        inner join "memberIdentities" mi on mem.id = mi."memberId" and mi.verified
        left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mem.id
        left join "memberEnrichmentCache" mec on mec."memberId" = mem.id
        where (
                (mi.verified and
                ((mi.type = 'username' AND mi.platform = 'github') OR (mi.type = 'email')))
                    OR
                ("membersGlobalActivityCount".total_count > 10 AND mi.type = 'email' and mi.verified)
                    OR
                (
                  ("membersGlobalActivityCount".total_count > 500) AND
                  (mem."displayName" like '% %') AND
                  (mem.attributes -> 'location' ->> 'default' is not null and
                   mem.attributes -> 'location' ->> 'default' <> '') AND
                  (
                    (mem.attributes -> 'websiteUrl' ->> 'default' is not null and
                    mem.attributes -> 'websiteUrl' ->> 'default' <> '') OR
                    (mi.verified AND mi.type = 'username' and mi.platform = 'github') OR
                    (mi.verified AND mi.type = 'email')
                  )
                )
                    OR
                ((mi.verified AND mi.type = 'username' and mi.platform = 'linkedin'))
              )
       group by mem.id
       order by mem.id desc)
    select count(*) as count from enrichable_in_at_least_one_source
 ),
 attempted_to_enrich_among_enrichable_members as (
    with enrichable_by_clearbit as (
        select distinct mec."memberId"
        from "memberEnrichmentCache" mec
        where mec.source = 'clearbit'
        and mec."memberId" in (
            select distinct mi."memberId"
            from "memberIdentities" mi
            where mi.verified and mi.type = 'email'
        )
    ),
    enrichable_by_progai as (
        select distinct mec."memberId"
        from "memberEnrichmentCache" mec
        where mec.source = 'progai'
        and mec."memberId" in (
            select distinct mi."memberId"
            from "memberIdentities" mi
            where mi.verified
            and mi.type = 'username'
            and mi.platform = 'github'
        )
    ),
    enrichable_by_serp as (
        select distinct mec."memberId"
        from "memberEnrichmentCache" mec
        where mec.source = 'serp'
        and mec."memberId" in (
            select distinct mem.id as "memberId"
            from members mem
                  inner join "memberIdentities" mi on mem.id = mi."memberId" and mi.verified
                  inner join "membersGlobalActivityCount"
                             on "membersGlobalActivityCount"."memberId" = mem.id
            where
                ("membersGlobalActivityCount".total_count > 500 and
                (mem."displayName" like '% %') and
                (mem.attributes -> 'location' ->> 'default' is not null and
                 mem.attributes -> 'location' ->> 'default' <> '') and
                ((mem.attributes -> 'websiteUrl' ->> 'default' is not null and
                  mem.attributes -> 'websiteUrl' ->> 'default' <> '') OR
                 (mi.verified and mi.type = 'username' and mi.platform = 'github') OR
                 (mi.verified and mi.type = 'email')))
            group by mem.id
            order by mem.id desc
        )
    ),
    enrichable_by_progai_linkedin_scraper as (
        with clearbit_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            where mec.source = 'clearbit'
              and mec.data is not null
              and mec.data -> 'linkedin' ->> 'handle' is not null),
        progai_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            where mec.source = 'progai'
              and mec.data is not null
              and mec.data ->> 'linkedin_url' is not null),
        serp_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            where mec.source = 'serp'
              and mec.data is not null
        ),
        existing_verified_linkedin_identities as (
            select distinct mi."memberId"
            from "memberIdentities" mi
            where mi.platform = 'linkedin' and mi.verified
            group by mi."memberId"),
        all_unique_members_enrichable_by_progai_linkedin_scraper as (
            select "memberId" from clearbit_linkedin_profiles
            union
            select "memberId" from progai_linkedin_profiles
            union
            select "memberId" from serp_linkedin_profiles
            union
            select "memberId" from existing_verified_linkedin_identities)
        select distinct "memberId"
        from all_unique_members_enrichable_by_progai_linkedin_scraper
    ),
   enrichable_by_crustdata_linkedin_scraper as (
        with clearbit_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'clearbit'
              and mec.data is not null
              and mec.data -> 'linkedin' ->> 'handle' is not null),
        progai_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'progai'
              and mec.data is not null
              and mec.data ->> 'linkedin_url' is not null),
        serp_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'serp'
              and mec.data is not null),
        existing_verified_linkedin_identities as (
            select distinct mi."memberId"
            from "memberIdentities" mi
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mi."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mi.verified
              and mi.platform = 'linkedin'
            group by mi."memberId"),
        all_unique_members_enrichable_by_crustdata as (
            select "memberId" from clearbit_linkedin_profiles
            union
            select "memberId" from progai_linkedin_profiles
            union
            select "memberId" from serp_linkedin_profiles
            union
            select "memberId" from existing_verified_linkedin_identities)
        select distinct "memberId" from all_unique_members_enrichable_by_crustdata
   ),
   unique_members as (
        select "memberId" from enrichable_by_clearbit
        union
        select "memberId" from enrichable_by_progai
        union
        select "memberId" from enrichable_by_serp
        union
        select "memberId" from enrichable_by_progai_linkedin_scraper
        union
        select "memberId" from enrichable_by_crustdata_linkedin_scraper)
   select count(distinct "memberId") as count
   from unique_members)
    SELECT (SELECT count FROM all_members)                                                                         as "totalMembers",
           (SELECT count FROM total_enrichable_members)                                                            as "enrichableMembers",
           (SELECT count FROM attempted_to_enrich_among_enrichable_members)                                       as "attemptedToEnrich";


-- member enrichment monitoring (clearbit)
drop materialized view if exists "memberEnrichmentMonitoringClearbit";
create materialized view "memberEnrichmentMonitoringClearbit" as
with clearbit_enrichable_members as (
    select count(distinct mi."memberId") as count
    from "memberIdentities" mi
    left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mi."memberId"
    where mi.verified and mi.type = 'email' and "membersGlobalActivityCount".total_count > 10
),
attempted_to_enrich_among_clearbit_enrichable_members as (
    select count(distinct mec."memberId") as count
    from "memberEnrichmentCache" mec
    where mec."memberId" in (
        select distinct mi."memberId"
        from "memberIdentities" mi
        left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mi."memberId"
        where mi.verified and mi.type = 'email' and "membersGlobalActivityCount".total_count > 10
    )
),
clearbit_hit_count_among_attempted as (
    select count(*) as count
    from "memberEnrichmentCache" mec
    where mec.data is not null
    and mec.source = 'clearbit'
    and mec."memberId" in (
        select distinct mi."memberId"
        from "memberIdentities" mi
        left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mi."memberId"
        where mi.verified and mi.type = 'email' and "membersGlobalActivityCount".total_count > 10
    )
)
select
      (select count from clearbit_enrichable_members) as "enrichableMembers",
      (select count from attempted_to_enrich_among_clearbit_enrichable_members) as "attemptedToEnrich",
      case when (select count from clearbit_enrichable_members)::numeric = 0 then 0 else 
      (round((select count from attempted_to_enrich_among_clearbit_enrichable_members)::numeric /
              (select count from clearbit_enrichable_members)::numeric * 100, 2)) end as progress,
       ((select count from clearbit_hit_count_among_attempted)) as "hitCount",
       case when (select count from attempted_to_enrich_among_clearbit_enrichable_members)::numeric = 0 then 0 else 
       (round((select count from clearbit_hit_count_among_attempted)::numeric /
              (select count from attempted_to_enrich_among_clearbit_enrichable_members)::numeric * 100,
              2)) end as "hitRate";


-- member enrichment monitoring (crustdata)
drop materialized view if exists "memberEnrichmentMonitoringCrustdata";
create materialized view "memberEnrichmentMonitoringCrustdata" as
with crustdata_members_with_scrapable_profiles AS (
    with clearbit_linkedin_profiles as (
        select distinct mec."memberId"
        from "memberEnrichmentCache" mec
        left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
        where "membersGlobalActivityCount".total_count > 1000
         and mec.source = 'clearbit'
         and mec.data is not null
         and mec.data -> 'linkedin' ->> 'handle' is not null),
    progai_linkedin_profiles as (
        select distinct mec."memberId"
         from "memberEnrichmentCache" mec
         left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
         where "membersGlobalActivityCount".total_count > 1000
           and mec.source = 'progai'
           and mec.data is not null
           and mec.data ->> 'linkedin_url' is not null),
    serp_linkedin_profiles as (
        select distinct mec."memberId"
        from "memberEnrichmentCache" mec
        left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
        where "membersGlobalActivityCount".total_count > 1000
         and mec.source = 'serp'
         and mec.data is not null),
    existing_verified_linkedin_identities as (
        select distinct mi."memberId"
        from "memberIdentities" mi
        left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mi."memberId"
        where "membersGlobalActivityCount".total_count > 1000
          and mi.verified
          and mi.platform = 'linkedin'
        group by mi."memberId"),
    unique_members AS (
        select "memberId" from clearbit_linkedin_profiles
        union
        select "memberId" from progai_linkedin_profiles
        union
        select "memberId" from serp_linkedin_profiles
        union
        select "memberId" from existing_verified_linkedin_identities)
select count(distinct "memberId") as count
from unique_members),
 crustdata_attempted_to_scrape_among_scrapable_profiles as (
    select count(*) as count
    from "memberEnrichmentCache" mec
    where mec.source = 'crustdata'
    and mec."memberId" in (
        with clearbit_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'clearbit'
              and mec.data is not null
              and mec.data -> 'linkedin' ->> 'handle' is not null),
        progai_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'progai'
              and mec.data is not null
              and mec.data ->> 'linkedin_url' is not null),
        serp_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'serp'
              and mec.data is not null),
        existing_verified_linkedin_identities as (
            select distinct mi."memberId"
            from "memberIdentities" mi
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mi."memberId"
            where "membersGlobalActivityCount".total_count > 1000 
              and mi.verified 
              and mi.platform = 'linkedin'
            group by mi."memberId"),
        unique_members as (
            select "memberId" from clearbit_linkedin_profiles
            union
            select "memberId" from progai_linkedin_profiles
            union
            select "memberId" from serp_linkedin_profiles
            union
            select "memberId" from existing_verified_linkedin_identities)
 select distinct "memberId" from unique_members)),
 crustdata_hit_count_among_attempted as (
    select count(*) as count
    from "memberEnrichmentCache" mec
    where mec.source = 'crustdata'
      and mec.data is not null
      and mec."memberId" in (
      with clearbit_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'clearbit'
              and mec.data is not null
              and mec.data -> 'linkedin' ->> 'handle' is not null),
      progai_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'progai'
              and mec.data is not null
              and mec.data ->> 'linkedin_url' is not null),
      serp_linkedin_profiles as (
            select distinct mec."memberId"
            from "memberEnrichmentCache" mec
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mec."memberId"
            where "membersGlobalActivityCount".total_count > 1000
              and mec.source = 'serp'
              and mec.data is not null),
      existing_verified_linkedin_identities as (
            select distinct mi."memberId"
            from "memberIdentities" mi
            left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mi."memberId"
            where "membersGlobalActivityCount".total_count > 1000 
            and mi.verified
            and mi.platform = 'linkedin'
            group by mi."memberId"),
      unique_members as (
            select "memberId" from clearbit_linkedin_profiles
            union
            select "memberId" from progai_linkedin_profiles
            union
            select "memberId" from serp_linkedin_profiles
            union
            select "memberId" from existing_verified_linkedin_identities)
select distinct "memberId" from unique_members))
select
      (select count from crustdata_members_with_scrapable_profiles) as "membersWithScrapableProfiles",
      (select count from crustdata_attempted_to_scrape_among_scrapable_profiles) as "attemptedToScrape",
      case when (select count from crustdata_members_with_scrapable_profiles)::numeric = 0 then 0 else 
      (round((select count from crustdata_attempted_to_scrape_among_scrapable_profiles)::numeric /
              (select count from crustdata_members_with_scrapable_profiles)::numeric * 100, 2)) end as progress,
       ((select count from crustdata_hit_count_among_attempted)) as "hitCount",
       case when (select count from crustdata_attempted_to_scrape_among_scrapable_profiles)::numeric = 0 then 0 else 
       (round((select count from crustdata_hit_count_among_attempted)::numeric /
              (select count from crustdata_attempted_to_scrape_among_scrapable_profiles)::numeric * 100,
              2)) end as "hitRate";

-- member enrichment monitoring (progai-github)
drop materialized view if exists "memberEnrichmentMonitoringProgaiGithub";
create materialized view "memberEnrichmentMonitoringProgaiGithub" as
with progai_enrichable_members as (
    select count(distinct mi."memberId") as count
    from "memberIdentities" mi
    where mi.verified
      and mi.type = 'username' and mi.platform = 'github'
),
attempted_to_enrich_among_progai_enrichable_members as (
    select count(distinct mec."memberId") as count
    from "memberEnrichmentCache" mec
    where mec."memberId" in (
        select distinct mi."memberId"
        from "memberIdentities" mi
        where mi.verified
          and mi.type = 'username' and mi.platform = 'github'
    )
),
progai_hit_count_among_attempted as (
    select count(*) as count
    from "memberEnrichmentCache" mec
    where mec.data is not null and mec.source = 'progai' and mec."memberId" in (
        select distinct mi."memberId"
        from "memberIdentities" mi
        where mi.verified
          and mi.type = 'username' and mi.platform = 'github'
    )
)
select
    (select count from progai_enrichable_members) as "enrichableMembers",
    (select count from attempted_to_enrich_among_progai_enrichable_members) as "attemptedToEnrich",
    case when (select count from progai_enrichable_members)::numeric = 0 then 0 else 
    round((select count from attempted_to_enrich_among_progai_enrichable_members)::numeric / (select count from progai_enrichable_members)::numeric * 100, 2) end as progress,
    (select count from progai_hit_count_among_attempted) as "hitCount",
    case when (select count from attempted_to_enrich_among_progai_enrichable_members)::numeric = 0 then 0 else 
    round((select count from progai_hit_count_among_attempted)::numeric / (select count from attempted_to_enrich_among_progai_enrichable_members)::numeric * 100, 2) end as "hitRate";

-- member enrichment monitoring (progai-linkedin)
drop materialized view if exists "memberEnrichmentMonitoringProgaiLinkedin";
create materialized view "memberEnrichmentMonitoringProgaiLinkedin" as
with progai_linkedin_members_with_scrapable_profiles as (
    with clearbit_linkedin_profiles as (
                 select distinct mec."memberId"
                 from "memberEnrichmentCache" mec
                 where mec.source = 'clearbit'
                   and mec.data is not null
                   and mec.data->'linkedin'->>'handle' is not null
    ),
    progai_linkedin_profiles as (
        select distinct mec."memberId"
                 from "memberEnrichmentCache" mec
                 where mec.source = 'progai'
                   and mec.data is not null
                   and mec.data->>'linkedin_url' is not null
    ),
    serp_linkedin_profiles as (
                 select distinct mec."memberId"
                 from "memberEnrichmentCache" mec
                 where mec.source = 'serp'
                   and mec.data is not null
    ),
    existing_verified_linkedin_identities as (
                 select distinct mi."memberId"
                 from "memberIdentities" mi
                 where mi.platform = 'linkedin'
                   and mi.verified
                 group by  mi."memberId"
    ),
    unique_members as (
            select "memberId" from clearbit_linkedin_profiles
            union
            select "memberId" from progai_linkedin_profiles
            union
            select "memberId" from serp_linkedin_profiles
            union
            select "memberId" from existing_verified_linkedin_identities
        )
    select count(distinct "memberId") as count from unique_members
),
progai_linkedin_attempted_to_scrape_among_scrapable_profiles as (
    select count(*) as count from "memberEnrichmentCache" mec where mec.source= 'progai-linkedin-scraper'
    and mec."memberId" in (
        with clearbit_linkedin_profiles as (
                 select distinct mec."memberId"
                 from "memberEnrichmentCache" mec
                 where mec.source = 'clearbit'
                   and mec.data is not null
                   and mec.data->'linkedin'->>'handle' is not null
        ),
        progai_linkedin_profiles as (
            select distinct mec."memberId"
                     from "memberEnrichmentCache" mec
                     where mec.source = 'progai'
                       and mec.data is not null
                       and mec.data->>'linkedin_url' is not null
        ),
        serp_linkedin_profiles as (
                     select distinct mec."memberId"
                     from "memberEnrichmentCache" mec
                     where mec.source = 'serp'
                       and mec.data is not null
        ),
        existing_verified_linkedin_identities as (
                     select distinct mi."memberId"
                     from "memberIdentities" mi
                     where mi.platform = 'linkedin'
                       and mi.verified
                     group by  mi."memberId"
        ),
        unique_members as (
                select "memberId" from clearbit_linkedin_profiles
                union
                select "memberId" from progai_linkedin_profiles
                union
                select "memberId" from serp_linkedin_profiles
                union
                select "memberId" from existing_verified_linkedin_identities
            )
    select distinct "memberId" from unique_members
    )
),
progai_linkedin_scraper_hit_count_among_attempted as (
    select count(*) as count from "memberEnrichmentCache" mec where mec.source= 'progai-linkedin-scraper'
    and mec.data is not null and mec."memberId" in (
        with clearbit_linkedin_profiles as (
                 select distinct mec."memberId"
                 from "memberEnrichmentCache" mec
                 where mec.source = 'clearbit'
                   and mec.data is not null
                   and mec.data->'linkedin'->>'handle' is not null
        ),
        progai_linkedin_profiles as (
            select distinct mec."memberId"
                     from "memberEnrichmentCache" mec
                     where mec.source = 'progai'
                       and mec.data is not null
                       and mec.data->>'linkedin_url' is not null
        ),
        serp_linkedin_profiles as (
                     select distinct mec."memberId"
                     from "memberEnrichmentCache" mec
                     where mec.source = 'serp' and mec.data is not null
        ),
        existing_verified_linkedin_identities as (
                     select distinct mi."memberId"
                     from "memberIdentities" mi
                     where mi.platform = 'linkedin'
                       and mi.verified
                     group by  mi."memberId"
        ),
        unique_members as (
                select "memberId" from clearbit_linkedin_profiles
                union
                select "memberId" from progai_linkedin_profiles
                union
                select "memberId" from serp_linkedin_profiles
                union
                select "memberId" from existing_verified_linkedin_identities
        )
    select distinct "memberId" from unique_members
    )
)
select
      (select count from progai_linkedin_members_with_scrapable_profiles) as "membersWithScrapableProfiles",
      (select count from progai_linkedin_attempted_to_scrape_among_scrapable_profiles) as "attemptedToScrape",
      case when (select count from progai_linkedin_members_with_scrapable_profiles)::numeric = 0 then 0 else 
      (round((select count from progai_linkedin_attempted_to_scrape_among_scrapable_profiles)::numeric /
              (select count from progai_linkedin_members_with_scrapable_profiles)::numeric * 100, 2)) end as progress,
       ((select count from progai_linkedin_scraper_hit_count_among_attempted)) as "hitCount",
      case when (select count from progai_linkedin_attempted_to_scrape_among_scrapable_profiles)::numeric = 0 then 0 else 
      (round((select count from progai_linkedin_scraper_hit_count_among_attempted)::numeric /
              (select count from progai_linkedin_attempted_to_scrape_among_scrapable_profiles)::numeric * 100,
              2)) end as "hitRate";

-- member enrichment monitoring (serp)
drop materialized view if exists "memberEnrichmentMonitoringSerp";
create materialized view "memberEnrichmentMonitoringSerp" as
with serp_enrichable_members as (
    select count(distinct mem.id) as count from members mem
    join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mem.id
    join "memberIdentities" mi on mi."memberId" = mem.id
    where
    ("membersGlobalActivityCount".total_count > 500) and
    (mem."displayName" like '% %') and
    (mem.attributes->'location'->>'default' is not null and mem.attributes->'location'->>'default' <> '') and
    ((mem.attributes->'websiteUrl'->>'default' is not null and mem.attributes->'websiteUrl'->>'default' <> '') or
     (mi.verified and mi.type = 'username' and mi.platform = 'github') or
     (mi.verified and mi.type = 'email')
    )
),
attempted_to_enrich_among_serp_enrichable_members as (
    select count(*) as count
    from "memberEnrichmentCache" mec
    where mec.source = 'serp' and mec."memberId" in (
            select distinct mem.id
            from members mem
            join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mem.id
            join "memberIdentities" mi on mi."memberId" = mem.id
            where
            ("membersGlobalActivityCount".total_count > 500) and
            (mem."displayName" like '% %') and
            (mem.attributes->'location'->>'default' is not null and mem.attributes->'location'->>'default' <> '') and
            ((mem.attributes->'websiteUrl'->>'default' is not null and mem.attributes->'websiteUrl'->>'default' <> '') or
            (mi.verified and mi.type = 'username' and mi.platform = 'github') or
            (mi.verified and mi.type = 'email'))
    )
),
serp_hit_count_among_attempted as (
    select count(*) as count
    from "memberEnrichmentCache" mec
    where mec.data is not null
    and mec.source = 'serp'
    and mec."memberId" in (
        select distinct mem.id from members mem
        join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mem.id
        join "memberIdentities" mi on mi."memberId" = mem.id
        where
        ("membersGlobalActivityCount".total_count > 500) and
        (mem."displayName" like '% %') and
        (mem.attributes->'location'->>'default' is not null and mem.attributes->'location'->>'default' <> '') and
        ((mem.attributes->'websiteUrl'->>'default' is not null and mem.attributes->'websiteUrl'->>'default' <> '') or
         (mi.verified and mi.type = 'username' and mi.platform = 'github') or
         (mi.verified and mi.type = 'email')
        )
    )
)
select
    (select count from serp_enrichable_members) as "enrichableMembers",
    (select count from attempted_to_enrich_among_serp_enrichable_members) as "attemptedToEnrich",
    case when (select count from serp_enrichable_members)::numeric = 0 then 0 else 
    round((select count from attempted_to_enrich_among_serp_enrichable_members)::numeric / (select count from serp_enrichable_members)::numeric * 100, 2) end as progress,
    (select count from serp_hit_count_among_attempted) as "hitCount",
    case when (select count from attempted_to_enrich_among_serp_enrichable_members)::numeric = 0 then 0 else 
    round((select count from serp_hit_count_among_attempted)::numeric / (select count from attempted_to_enrich_among_serp_enrichable_members)::numeric * 100, 2) end as "hitRate";


-- member enrichment monitoring (entity updates)
drop materialized view if exists "memberEnrichmentMonitoringEntityUpdates";
create materialized view "memberEnrichmentMonitoringEntityUpdates" as
with enriched_total as (
    WITH total_members as (
        select count(*) as count
        from "memberEnrichments"
        WHERE "lastUpdatedAt" is not null
    ),
    members_with_more_than_1000_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 1000
          AND m."lastUpdatedAt" is not null
    ),
    members_with_more_than_100_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 100 and act_count.total_count < 1000
          AND m."lastUpdatedAt" is not null
    ),
    members_with_more_than_10_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 10 and act_count.total_count < 100
          AND m."lastUpdatedAt" is not null
    )
    select
        (select count from total_members) as "enriched_today_total",
        (select count from members_with_more_than_1000_activity) as "enriched_today_members_more_than_1000_activity",
        (select count from members_with_more_than_100_activity) as "enriched_today_members_more_than_100_activity",
        (select count from members_with_more_than_10_activity) as "enriched_today_members_more_than_10_activity"
),
enriched_today as (
    WITH total_members as (
        select count(*) as count
        from "memberEnrichments"
        WHERE "lastUpdatedAt" >= date_trunc('day', now())
    ),
    members_with_more_than_1000_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 1000
          AND m."lastUpdatedAt" >= date_trunc('day', now())
    ),
    members_with_more_than_100_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 100 and act_count.total_count < 1000
          AND m."lastUpdatedAt" >= date_trunc('day', now())
    ),
    members_with_more_than_10_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 10  and act_count.total_count < 100
          AND m."lastUpdatedAt" >= date_trunc('day', now())
    )
    select
        (select count from total_members) as "enriched_today_total",
        (select count from members_with_more_than_1000_activity) as "enriched_today_members_more_than_1000_activity",
        (select count from members_with_more_than_100_activity) as "enriched_today_members_more_than_100_activity",
        (select count from members_with_more_than_10_activity) as "enriched_today_members_more_than_10_activity"
),
enriched_since_yesterday as (
        WITH total_members as (
        select count(*) as count
        from "memberEnrichments"
        WHERE "lastUpdatedAt" >= date_trunc('day', now() - interval '1 day')
    ),
    members_with_more_than_1000_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 1000
          AND m."lastUpdatedAt" >= date_trunc('day', now() - interval '1 day')
    ),
    members_with_more_than_100_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 100 and act_count.total_count < 1000
          AND m."lastUpdatedAt" >= date_trunc('day', now() - interval '1 day')
    ),
    members_with_more_than_10_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 10  and act_count.total_count < 100
          AND m."lastUpdatedAt" >= date_trunc('day', now() - interval '1 day')
    )
    select
        (select count from total_members) as "enriched_since_yesterday_total",
        (select count from members_with_more_than_1000_activity) as "enriched_since_yesterday_members_more_than_1000_activity",
        (select count from members_with_more_than_100_activity) as "enriched_since_yesterday_members_more_than_100_activity",
        (select count from members_with_more_than_10_activity) as "enriched_since_yesterday_members_more_than_10_activity"
),
enriched_in_last_30days as (
       WITH total_members as (
        select count(*) as count
        from "memberEnrichments"
        WHERE "lastUpdatedAt" >= now() - interval '30 days'
    ),
    members_with_more_than_1000_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 1000
          AND m."lastUpdatedAt" >= now() - interval '30 days'
    ),
    members_with_more_than_100_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 100 and act_count.total_count < 1000
          AND m."lastUpdatedAt" >= now() - interval '30 days'
    ),
    members_with_more_than_10_activity as (
        select count(*) as count
        from "memberEnrichments" m
        LEFT JOIN "membersGlobalActivityCount" act_count
        ON act_count."memberId" = m."memberId"
        WHERE act_count.total_count > 10  and act_count.total_count < 100
          AND m."lastUpdatedAt" >= now() - interval '30 days'
    )
    select
        (select count from total_members) as "enriched_in30d_total",
        (select count from members_with_more_than_1000_activity) as "enriched_in30d_members_more_than_1000_activity",
        (select count from members_with_more_than_100_activity) as "enriched_in30d_members_more_than_100_activity",
        (select count from members_with_more_than_10_activity) as "enriched_in30d_members_more_than_10_activity"
),
last_enriched_3_profiles as (
    select 'https://cm.lfx.dev/people/' || m."memberId" || '?projectGroup=dc48fac5-b31a-4659-ac99-60eb52a1082a' as profiles
    from "memberEnrichments" m
    where exists (
        select 1 from "memberEnrichmentCache" mec
                 where mec."memberId" = m."memberId"
                 and data is not null
    )
    and m."lastUpdatedAt" is not null
    order by m."lastUpdatedAt" desc
    limit 3
),
last_enriched_3_profiles_with_more_than_1000_activities as (
    select 'https://cm.lfx.dev/people/' || m."memberId" || '?projectGroup=dc48fac5-b31a-4659-ac99-60eb52a1082a' as profiles
    from "memberEnrichments" m
    left join "membersGlobalActivityCount" act_count on act_count."memberId" = m."memberId"
    where act_count.total_count > 1000
    and exists (
        select 1 from "memberEnrichmentCache" mec
                 where mec."memberId" = m."memberId"
                 and data is not null
    )
    and m."lastUpdatedAt" is not null
    order by m."lastUpdatedAt" desc
    limit 3
),
last_enriched_3_profiles_with_more_than_100_activities as (
    select 'https://cm.lfx.dev/people/' || m."memberId" || '?projectGroup=dc48fac5-b31a-4659-ac99-60eb52a1082a' as profiles
    from "memberEnrichments" m
    left join "membersGlobalActivityCount" act_count on act_count."memberId" = m."memberId"
    where act_count.total_count > 100 and act_count.total_count < 1000
    and exists (
        select 1 from "memberEnrichmentCache" mec
                 where mec."memberId" = m."memberId"
                 and data is not null
    )
    and m."lastUpdatedAt" is not null
    order by m."lastUpdatedAt" desc
    limit 3
),
last_enriched_3_profiles_with_more_than_10_activities as (
    select 'https://cm.lfx.dev/people/' || m."memberId" || '?projectGroup=dc48fac5-b31a-4659-ac99-60eb52a1082a' as profiles
    from "memberEnrichments" m
    left join "membersGlobalActivityCount" act_count on act_count."memberId" = m."memberId"
    where act_count.total_count > 10 and act_count.total_count < 100
    and exists (
        select 1 from "memberEnrichmentCache" mec
                 where mec."memberId" = m."memberId"
                 and data is not null
    )
    and m."lastUpdatedAt" is not null
    order by m."lastUpdatedAt" desc
    limit 3
),
oldest_created_at_of_enrichable_member as (
    with enrichable_in_at_least_one_source as (
        select mem.id, mem."createdAt", max("membersGlobalActivityCount".total_count) as total_count
        from members mem
        inner join "memberIdentities" mi on mem.id = mi."memberId" and mi.verified
        left join "membersGlobalActivityCount" on "membersGlobalActivityCount"."memberId" = mem.id
        left join "memberEnrichmentCache" mec on mec."memberId" = mem.id
        where (
                (mi.verified and
                ((mi.type = 'username' AND mi.platform = 'github') OR (mi.type = 'email')))
                    OR
                ("membersGlobalActivityCount".total_count > 10 AND mi.type = 'email' and mi.verified)
                    OR
                (
                  ("membersGlobalActivityCount".total_count > 500) AND
                  (mem."displayName" like '% %') AND
                  (mem.attributes -> 'location' ->> 'default' is not null and
                   mem.attributes -> 'location' ->> 'default' <> '') AND
                  (
                    (mem.attributes -> 'websiteUrl' ->> 'default' is not null and
                    mem.attributes -> 'websiteUrl' ->> 'default' <> '') OR
                    (mi.verified AND mi.type = 'username' and mi.platform = 'github') OR
                    (mi.verified AND mi.type = 'email')
                  )
                )
                    OR
                ((mi.verified AND mi.type = 'username' and mi.platform = 'linkedin'))
              )
       group by mem.id
       order by mem.id desc)
        select
            (select min("createdAt") as "date" from enrichable_in_at_least_one_source) as total,
            (select min("createdAt") as "date" from enrichable_in_at_least_one_source where total_count > 1000) as members_with_more_than_1000_activities,
            (select min("createdAt") as "date" from enrichable_in_at_least_one_source where total_count > 100 and total_count < 1000) as members_with_more_than_100_activities,
            (select min("createdAt") as "date" from enrichable_in_at_least_one_source where total_count > 10 and total_count < 100) as members_with_more_than_10_activities
)
    select
        (select enriched_today_total from enriched_total) as "enrichedTotalAll",
        (select enriched_today_members_more_than_1000_activity from enriched_total) as "enrichedTotal1000",
        (select enriched_today_members_more_than_100_activity from enriched_total) as "enrichedTotal100",
        (select enriched_today_members_more_than_10_activity from enriched_total) as "enrichedTotal10",

        (select enriched_today_total from enriched_today) as "enrichedTodayAll",
        (select enriched_today_members_more_than_1000_activity from enriched_today) as "enrichedToday1000",
        (select enriched_today_members_more_than_100_activity from enriched_today) as "enrichedToday100",
        (select enriched_today_members_more_than_10_activity from enriched_today) as "enrichedToday10",

        (select enriched_since_yesterday_total from enriched_since_yesterday) as "enrichedSinceYesterdayAll",
        (select enriched_since_yesterday_members_more_than_1000_activity from enriched_since_yesterday) as "enrichedSinceYesterday1000",
        (select enriched_since_yesterday_members_more_than_100_activity from enriched_since_yesterday) as "enrichedSinceYesterday100",
        (select enriched_since_yesterday_members_more_than_10_activity from enriched_since_yesterday) as "enrichedSinceYesterday10",

        (select enriched_in30d_total from enriched_in_last_30days) as "enrichedInLast30DaysAll",
        (select enriched_in30d_members_more_than_1000_activity from enriched_in_last_30days) as "enrichedInLast30Days1000",
        (select enriched_in30d_members_more_than_100_activity from enriched_in_last_30days) as "enrichedInLast30Days100",
        (select enriched_in30d_members_more_than_10_activity from enriched_in_last_30days) as "enrichedInLast30Days10",

        (select array_agg(profiles) from last_enriched_3_profiles ) as "lastEnriched3ProfilesAll",
        (select array_agg(profiles) from last_enriched_3_profiles_with_more_than_1000_activities ) as "lastEnriched3Profiles1000",
        (select array_agg(profiles) from last_enriched_3_profiles_with_more_than_100_activities ) as "lastEnriched3Profiles100",
        (select array_agg(profiles) from last_enriched_3_profiles_with_more_than_10_activities ) as "lastEnriched3Profiles10",

        (select total from oldest_created_at_of_enrichable_member) as "oldestCreatedAtInEnrichableMembersAll",
        (select members_with_more_than_1000_activities from oldest_created_at_of_enrichable_member) as "oldestCreatedAtInEnrichableMembers1000",
        (select members_with_more_than_100_activities from oldest_created_at_of_enrichable_member) as "oldestCreatedAtInEnrichableMembers100",
        (select members_with_more_than_10_activities from oldest_created_at_of_enrichable_member) as "oldestCreatedAtInEnrichableMembers10";


-- member enrichment monitoring (LLM queries)
drop materialized view if exists "memberEnrichmentMonitoringLLMQueries";
create materialized view "memberEnrichmentMonitoringLLMQueries" as
with check_profile_belongs_to_member_with_llm as (
    with total_times_called as (
        select count(*) as count
        from "llmPromptHistory"
        where type = 'member_enrichment_find_related_linkedin_profiles'
    ),
    averages as (
        select avg("inputTokenCount") as "inputToken",
               sum("inputTokenCount") as "inputTokenTotal",
               avg("outputTokenCount") as "outputToken",
               sum("outputTokenCount") as "outputTokenTotal",
               avg("responseTimeSeconds") as "responseTimeSeconds"  
        from "llmPromptHistory"
        where type = 'member_enrichment_find_related_linkedin_profiles'
    )
    select
        (select count from total_times_called) as "total_times_called",
        (select ("inputToken" * 0.003 + "outputToken" * 0.015) / 1000 from averages) as "average_cost",
        (select "responseTimeSeconds" from averages) as "average_response_time",
        (select ("inputTokenTotal" * 0.003 + "outputTokenTotal" * 0.015) / 1000 from averages) as "total_cost"
),
squash_multiple_value_attributes_with_llm as (
    with total_times_called as (
        select count(*) as count
        from "llmPromptHistory"
        where type = 'member_enrichment_squash_multiple_value_attributes'
    ),
    averages as (
        select avg("inputTokenCount") as "inputToken",
               sum("inputTokenCount") as "inputTokenTotal",
               avg("outputTokenCount") as "outputToken",
               sum("outputTokenCount") as "outputTokenTotal",
               avg("responseTimeSeconds") as "responseTimeSeconds"
        from "llmPromptHistory"
        where type = 'member_enrichment_squash_multiple_value_attributes'
    )
    select
        (select count from total_times_called) as "total_times_called",
        (select ("inputToken" * 0.003 + "outputToken" * 0.015) / 1000 from averages) as "average_cost",
        (select "responseTimeSeconds" from averages) as "average_response_time",
        (select ("inputTokenTotal" * 0.003 + "outputTokenTotal" * 0.015) / 1000 from averages) as "total_cost"
),
squash_work_experiences_with_llm as (
    with total_times_called as (
        select count(*) as count
        from "llmPromptHistory"
        where type = 'member_enrichment_squash_work_experiences_from_multiple_sources'
        ),
        averages as (
        select avg("inputTokenCount") as "inputToken",
               sum("inputTokenCount") as "inputTokenTotal",
               avg("outputTokenCount") as "outputToken",
               sum("outputTokenCount") as "outputTokenTotal",
               avg("responseTimeSeconds") as "responseTimeSeconds"
            from "llmPromptHistory"
            where type = 'member_enrichment_squash_work_experiences_from_multiple_sources'
        )
        select
            (select count from total_times_called) as "total_times_called",
            (select ("inputToken" * 0.003 + "outputToken" * 0.015) / 1000 from averages) as "average_cost",
            (select "responseTimeSeconds" from averages) as "average_response_time",
            (select ("inputTokenTotal" * 0.003 + "outputTokenTotal" * 0.015) / 1000 from averages) as "total_cost"
)
    select
        (select total_times_called from check_profile_belongs_to_member_with_llm) as "checkProfileBelongsToMemberTotalTimesCalled",
        (select round(average_cost, 5) from check_profile_belongs_to_member_with_llm) as "checkProfileBelongsToMemberAvgCostPerRequest",
        (select round(average_response_time, 2) from check_profile_belongs_to_member_with_llm) as "checkProfileBelongsToMemberAvgResponseTime",
        (select round(total_cost, 2) from check_profile_belongs_to_member_with_llm) as "checkProfileBelongsToMemberTotalCost",

        (select total_times_called from squash_multiple_value_attributes_with_llm) as "squashAttributesTotalTimesCalled",
        (select round(average_cost, 5) from squash_multiple_value_attributes_with_llm) as "squashAttributesAvgCostPerRequest",
        (select round(average_response_time, 2) from squash_multiple_value_attributes_with_llm) as "squashAttributesAvgResponseTime",
        (select round(total_cost, 2) from squash_multiple_value_attributes_with_llm) as "squashAttributesTotalCost",

        (select total_times_called from squash_work_experiences_with_llm) as "squashWorkExperiencesTotalTimesCalled",
        (select round(average_cost, 5) from squash_work_experiences_with_llm) as "squashWorkExperiencesAvgCostPerRequest",
        (select round(average_response_time, 2) from squash_work_experiences_with_llm) as "squashWorkExperiencesAvgResponseTime",
        (select round(total_cost, 2) from squash_work_experiences_with_llm) as "squashWorkExperiencesTotalCost";


