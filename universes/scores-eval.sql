-- Validation Table
set query_group to 'importers';
set wlm_query_slot_count to 1;
DROP TABLE IF EXISTS bernie_nmarchio2.base_universe_qc;
CREATE TABLE bernie_nmarchio2.base_universe_qc
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_validation_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,county_fips
         ,county_name
         ,voting_address_latitude
         ,voting_address_longitude
         ,party_8way
         ,party_3way
         ,civis_2020_partisanship
         ,vote_history_6way
         ,early_vote_history_3way
         ,registered_in_state_3way
         ,dem_primary_eligible_2way
         ,race_5way
         ,spanish_language_2way
         ,age_5way
         ,ideology_5way
         ,education_2way
         ,income_5way
         ,gender_2way
         ,urban_3way
         ,child_in_hh_2way
         ,marital_2way
         ,religion_9way
         ,electorate_2way
         ,vote_ready_5way
         ,thirdp_support_1_id
         ,thirdp_support_2_id
         ,thirdp_support_1_2_id
         ,thirdp_support_3_id
         ,thirdp_support_4_id
         ,thirdp_support_5_id
         ,thirdp_support_1_2_3_4_5_id
         ,thirdp_holdout_group
         ,thirdp_first_choice_bernie_hh
         ,thirdp_first_choice_trump_hh
         ,thirdp_first_choice_biden_warren_buttigieg_hh
         ,thirdp_first_choice_any_hh
         ,thirdp_support_1_id_hh
         ,thirdp_support_2_id_hh
         ,thirdp_support_3_id_hh
         ,thirdp_support_4_id_hh
         ,thirdp_support_5_id_hh
         ,thirdp_support_1_2_3_4_5_id_hh
         ,ccj_contactdate
         ,ccj_contacted_last_30_days
         ,ccj_contact_made
         ,ccj_negative_result
         ,ccj_id_1
         ,ccj_id_1_2
         ,ccj_id_2
         ,ccj_id_3
         ,ccj_id_4
         ,ccj_id_5
         ,ccj_id_1_2_3_4_5
         ,ccj_holdout_group
         ,ccj_id_1_hh
         ,ccj_id_2_hh
         ,ccj_id_3_hh
         ,ccj_id_4_hh
         ,ccj_id_5_hh
         ,ccj_id_1_2_3_4_5_hh
         ,activist_flag
         ,activist_household_flag
         ,donor_1plus_flag
         ,donor_1plus_household_flag
         ,pturnout_2008
         ,pturnout_2016
         ,support_guardrail_validation
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, field_id_1_score DESC) as gotv_validation_rank
from bernie_data_commons.base_universe where dem_primary_eligible_2way  = '1 - Dem Primary Eligible'));

-- Scores Eval Query
select 
state_code,
ccj_holdout_group,
gotv_tiers_20,
coalesce(number_of_voters,0) as number_of_voters,
sum(coalesce(number_of_voters,0)) over (partition BY state_code||gotv_tiers_20) as number_of_voters_in_ventile,
coalesce(activists,0) as activists,
sum(coalesce(activists,0)) over (partition BY state_code||gotv_tiers_20) as activists_in_ventile,
round(coalesce(1.0*ccj1/ccj1all,0),4) as ccj1rate,
round(coalesce(1.0*ccj12/ccj1all,0),4) as ccj12rate,
coalesce(activists,0) as activists,
coalesce(ccj1,0) as ccj1,
coalesce(ccj2,0) as ccj2,
coalesce(ccj3,0) as ccj3,
coalesce(ccj4,0) as ccj4,
coalesce(ccj5,0) as ccj5,
coalesce(ccj1all,0) as ccj1all,
coalesce(ccj12,0) as ccj12
from
(select
state_code,
ccj_holdout_group,
gotv_tiers_20,
count(*) as number_of_voters,
sum(case when activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_flag_household_flag = 1 then 1 end) as activists,
sum(ccj_id_1) as ccj1,
sum(ccj_id_2) as ccj2,
sum(ccj_id_3) as ccj3,
sum(ccj_id_4) as ccj4,
sum(ccj_id_5) as ccj5,
sum(ccj_id_1_2_3_4_5) as ccj1all,
sum(ccj_id_1_2) as ccj12,
avg(field_id_1_score_100) as field1score,
avg(current_support_raw_100) as supportscore
from 
bernie_nmarchio2.base_universe_qc  group by 1,2,3) order by 1,2,3

