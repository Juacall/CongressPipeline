-- stg_members.sql
--
-- Clean and rename raw_members.
-- One row per House member from a target district.
{{ config(materialized = 'view') }}
select
    bioguide_id                 as member_id,     
    trim(name)                  as member_name,
    upper(trim(state))          as state_code,    
    cast(district as integer)   as district_number, 
    trim(party)                 as party_name,
    geoid_cd                    as geoid_cd  
from {{ source('congress', 'raw_members') }}
where
    bioguide_id is not null
    and trim(bioguide_id) != ''
    and state is not null
    and district is not null
    and geoid_cd is not null
