-- stg_bills.sql
--
-- Clean and rename raw_bills.
-- One row per member-bill relationship — a bill appears once per connected member.
-- member_relationship is either 'sponsor' or 'cosponsor'.
{{ config(materialized = 'view') }}

select
    cast(congress as integer)               as congress,
    upper(trim(bill_type))                  as bill_type,
    trim(bill_number)                       as bill_number,
    upper(trim(bill_type))
        || '-' || trim(bill_number)         as bill_id,
    trim(title)                             as bill_title,
    trim(member_id)                         as member_id,
    lower(trim(relationship))               as member_relationship,  -- 'sponsor' or 'cosponsor'
    case upper(trim(bill_type))
        when 'HR'      then 'House Bill'
        when 'HRES'    then 'House Resolution'
        when 'HJRES'   then 'House Joint Resolution'
        when 'HCONRES' then 'House Concurrent Resolution'
        else upper(trim(bill_type))
    end                                     as bill_type_label
from {{ source('congress', 'raw_bills') }}
where
    member_id is not null
    and trim(member_id) != ''
    and bill_type is not null
    and bill_number is not null
    and trim(bill_number) != ''
    and congress is not null
    and upper(trim(bill_type)) in ('HR', 'HRES', 'HJRES', 'HCONRES')
    and lower(trim(relationship)) in ('sponsor', 'cosponsor')

