-- stg_members.sql
--
-- Clean and rename raw_members.
-- Includes members from target districts and states.
{{ config(materialized = 'view') }}
select
    bioguide_id                 as member_id,     
    trim(name)                  as member_name,
    upper(trim(state))          as state_code,
    trim(chamber)               as chamber,
    cast(district as integer)   as district_number, 
    trim(party)                 as party_name,
    geoid_cd                    as geoid_cd  
from {{ source('congress', 'raw_members') }}
where
    bioguide_id is not null
    and trim(bioguide_id) != ''
    and state is not null
