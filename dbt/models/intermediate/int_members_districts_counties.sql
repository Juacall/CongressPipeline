-- int_members_districts_counties.sql
-- GRAIN: one row per member x county (Nationwide).

{{ config(materialized = 'table') }}

with house_members as (

    select distinct
        m.member_id,
        m.chamber,
        m.member_name,
        m.state_code,
        m.district_number,
        m.party_name,
        m.geoid_cd,
        census.GEOID_COUNTY_20 as county_fips
    from {{ ref('stg_members') }} as m
    inner join {{ ref('raw_census__cd11920_county20') }} as census
        on m.geoid_cd = census.GEOID_CD119_20
    where lower(m.chamber) = 'house'

),

senate_members as (

    select distinct
        m.member_id,
        m.chamber,
        m.member_name,
        m.state_code,
        m.district_number,  -- NULL for Senators
        m.party_name,
        m.geoid_cd,          -- NULL for Senators
        census.GEOID_COUNTY_20 as county_fips
    from {{ ref('stg_members') }} as m
    inner join {{ ref('raw_census__cd11920_county20') }} as census
        -- Senators represent all counties in their state (matching state FIPS code)
        on upper(m.state_code) = upper(census.STATE_ABBR)
        or lpad(cast(census.STATE_FIPS as varchar), 2, '0') = upper(m.state_code)
    where lower(m.chamber) = 'senate'

)

select * from house_members
union all
select * from senate_members