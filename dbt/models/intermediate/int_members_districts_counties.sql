-- int_members_districts_counties.sql
--
-- GRAIN: one row per member x county.
--
-- Links House members to the target counties in their districts. A district
-- can overlap multiple target counties, so a member may appear more than once.
--
-- county_fips is the full 5-digit FIPS (state + county) e.g. '13047'
{{ config(materialized = 'table') }}

select distinct
    m.member_id,
    m.member_name,
    m.state_code,
    m.district_number,
    m.party_name,
    m.geoid_cd,
    lpad(cast(tc.state_fips  as varchar), 2, '0')
    || lpad(cast(tc.county_fips as varchar), 3, '0') as county_fips,
    tc.county_name,
    tc.state as county_state
from {{ ref('stg_members') }} as m
inner join {{ ref('raw_census__cd11920_county20') }} as census
    on m.geoid_cd = census.GEOID_CD119_20
inner join {{ ref('target_counties') }} as tc
    on census.GEOID_COUNTY_20 =
       lpad(cast(tc.state_fips  as varchar), 2, '0')
    || lpad(cast(tc.county_fips as varchar), 3, '0')
where lower(m.chamber) = 'house'
