

-- Eligible Dem Primary Voters and high support targets
set query_group to 'importers';
set wlm_query_slot_count to 1;
-- Broadest universe
CREATE TABLE gotv_universes.ut_gotv_electorate
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||electorate_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,electorate_2way
         ,support_guardrail
         ,round(1.0*count(*) OVER (partition BY state_code||electorate_2way ORDER BY support_guardrail ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||electorate_2way  ORDER BY support_guardrail ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe where electorate_2way  = '1 - Target universe' and state_code = 'UT'));

-- Only eligible Dem Primary Voters
CREATE TABLE gotv_universes.ut_gotv_dem_primary_eligible
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' and state_code = 'UT'));

-- Only highly active voters with history voting in primaries who are Dem Primary eligible
CREATE TABLE gotv_universes.ut_gotv_vote_ready
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||vote_ready_5way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,vote_ready_5way
         ,support_guardrail
         ,round(1.0*count(*) OVER (partition BY state_code||vote_ready_5way ORDER BY support_guardrail ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||vote_ready_5way  ORDER BY support_guardrail ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe where vote_ready_5way =  '1 - Vote-ready' and state_code = 'UT'));

-- Custom tiers that insert millenials at top of rank order with activists, donors, and verified supporters
CREATE TABLE gotv_universes.ut_gotv_dem_primary_eligible_millennials
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1
          OR ccj_id_1 = 1
          OR thirdp_support_1_id = 1
          OR thirdp_first_choice_bernie = 1
          OR age_5way = '1 - 18-34' then '0 - Donors, Activists, Supporters, Millennials'
        when field_id_1_score_100 >= 70 
          or field_id_composite_score_100 >= 70 
          or current_support_raw_100 >= 70
          or sanders_strong_support_score_100 >= 70 
          or student_flag = 1 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as custom_tiers
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' and state_code = 'UT'));

