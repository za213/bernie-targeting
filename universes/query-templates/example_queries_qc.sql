/*
The below is an example of how to compare the various lists created in the last query template examples.
*/


drop table if exists bernie_nmarchio2.ut_gotv_dem_primary_eligible_early_voters;
drop table if exists bernie_nmarchio2.ut_gotv_dem_primary_eligible_likely_dems;
drop table if exists bernie_nmarchio2.ut_gotv_dem_primary_eligible_millennials;
drop table if exists bernie_nmarchio2.ut_gotv_dem_primary_eligible_vanilla;
drop table if exists bernie_nmarchio2.ut_gotv_dem_primary_eligible_vote_ready;

begin;

-- Only eligible Dem Primary Voters 
CREATE TABLE bernie_nmarchio2.ut_gotv_dem_primary_eligible_vanilla
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail_validation
         ,activist_flag
         ,activist_household_flag
         ,donor_1plus_flag
         ,donor_1plus_household_flag
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
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT'
and support_guardrail_validation IN ('0 - Donors and Activists','1 - Inside Support Guardrail')));

commit;

begin;


-- Only eligible Dem Primary Voters who are likely Democrats
CREATE TABLE bernie_nmarchio2.ut_gotv_dem_primary_eligible_likely_dems
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail_validation
         ,activist_flag
         ,activist_household_flag
         ,donor_1plus_flag
         ,donor_1plus_household_flag
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
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail_validation IN ('0 - Donors and Activists','1 - Inside Support Guardrail') ));

commit;

begin;

-- Only eligible Dem Primary Voters who are Democrats and prioritizing early voters first
CREATE TABLE bernie_nmarchio2.ut_gotv_dem_primary_eligible_early_voters
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,early_vote_history_3way
         ,support_guardrail_validation
         ,activist_flag
         ,activist_household_flag
         ,donor_1plus_flag
         ,donor_1plus_household_flag
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
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, early_vote_history_3way ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, early_vote_history_3way ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail_validation IN ('0 - Donors and Activists','1 - Inside Support Guardrail') ));

commit;

begin;

-- Only eligible Dem Primary Voters who are Democrats and limiting to more active voters
CREATE TABLE bernie_nmarchio2.ut_gotv_dem_primary_eligible_vote_ready
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,vote_ready_5way
         ,support_guardrail_validation
         ,activist_flag
         ,activist_household_flag
         ,donor_1plus_flag
         ,donor_1plus_household_flag
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
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, early_vote_history_3way ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY support_guardrail_validation ASC, early_vote_history_3way ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail_validation IN ('0 - Donors and Activists','1 - Inside Support Guardrail') 
and vote_ready_5way =  '1 - Vote-ready'));

commit;

begin;

-- Custom tiers that insert millenials at top of rank order with activists, donors, and verified supporters
CREATE TABLE bernie_nmarchio2.ut_gotv_dem_primary_eligible_millennials
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY gotv_rank ASC) AS gotv_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail_validation
         ,activist_flag
         ,activist_household_flag
         ,donor_1plus_flag
         ,donor_1plus_household_flag
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
         ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1
          OR age_5way = '1 - 18-34' then '0 - Donors and Activists, Millennials'
        when field_id_1_score_100 >= 70 
          or field_id_composite_score_100 >= 70 
          or current_support_raw_100 >= 70
          or sanders_strong_support_score_100 >= 70 
          or student_flag = 1 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as custom_tiers
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, field_id_1_score DESC) as gotv_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and state_code = 'UT' 
and (civis_2020_partisanship >= .66 or party_8way = '1 - Democratic')
and support_guardrail_validation IN ('0 - Donors and Activists','1 - Inside Support Guardrail')));

commit;



begin;

CREATE TABLE bernie_nmarchio2.ut_gotv_list_validation as
(select 
state_code,
ccj_holdout_group,
list_version,
gotv_tiers_20,
coalesce(number_of_voters,0) as number_of_voters,
sum(coalesce(number_of_voters,0)) over (partition BY state_code||list_version||gotv_tiers_20) as number_of_voters_in_ventile,
coalesce(activists,0) as activists,
sum(coalesce(activists,0)) over (partition BY state_code||list_version||gotv_tiers_20) as activists_in_ventile,
round(coalesce(1.0*ccj1/ccjall,0),4) as ccj1rate,
coalesce(ccj1,0) as ccj1,
coalesce(ccj2,0) as ccj2,
coalesce(ccj3,0) as ccj3,
coalesce(ccj4,0) as ccj4,
coalesce(ccj5,0) as ccj5,
coalesce(ccjall,0) as ccjall
from
((select state_code
        ,gotv_tiers_20
        ,'1 - Vanilla' as list_version
        ,ccj_holdout_group
        ,count(*) as number_of_voters
        ,sum(case when activist_flag = 1
                  OR activist_household_flag = 1
                  OR donor_1plus_flag = 1 
                  OR donor_1plus_household_flag = 1 then 1 end) as activists
        ,sum(ccj_id_1) as ccj1
        ,sum(ccj_id_2) as ccj2
        ,sum(ccj_id_3) as ccj3
        ,sum(ccj_id_4) as ccj4
        ,sum(ccj_id_5) as ccj5
        ,sum(ccj_id_1_2_3_4_5) as ccjall from bernie_nmarchio2.ut_gotv_dem_primary_eligible_vanilla group by 1,2,3,4)
union all 
(select state_code
        ,gotv_tiers_20
        ,'2 - Likely Dems' as list_version
        ,ccj_holdout_group
        ,count(*) as number_of_voters
        ,sum(case when activist_flag = 1
                  OR activist_household_flag = 1
                  OR donor_1plus_flag = 1 
                  OR donor_1plus_household_flag = 1 then 1 end) as activists
        ,sum(ccj_id_1) as ccj1
        ,sum(ccj_id_2) as ccj2
        ,sum(ccj_id_3) as ccj3
        ,sum(ccj_id_4) as ccj4
        ,sum(ccj_id_5) as ccj5
        ,sum(ccj_id_1_2_3_4_5) as ccjall from bernie_nmarchio2.ut_gotv_dem_primary_eligible_likely_dems group by 1,2,3,4)
union all
(select state_code
        ,gotv_tiers_20
        ,'3 - Early voters' as list_version
        ,ccj_holdout_group
        ,count(*) as number_of_voters
        ,sum(case when activist_flag = 1
                  OR activist_household_flag = 1
                  OR donor_1plus_flag = 1 
                  OR donor_1plus_household_flag = 1 then 1 end) as activists
        ,sum(ccj_id_1) as ccj1
        ,sum(ccj_id_2) as ccj2
        ,sum(ccj_id_3) as ccj3
        ,sum(ccj_id_4) as ccj4
        ,sum(ccj_id_5) as ccj5
        ,sum(ccj_id_1_2_3_4_5) as ccjall from bernie_nmarchio2.ut_gotv_dem_primary_eligible_early_voters group by 1,2,3,4)
union all
(select state_code
        ,gotv_tiers_20
        ,'4 - Custom millennials' as list_version
        ,ccj_holdout_group
        ,count(*) as number_of_voters
        ,sum(case when activist_flag = 1
                  OR activist_household_flag = 1
                  OR donor_1plus_flag = 1 
                  OR donor_1plus_household_flag = 1 then 1 end) as activists
        ,sum(ccj_id_1) as ccj1
        ,sum(ccj_id_2) as ccj2
        ,sum(ccj_id_3) as ccj3
        ,sum(ccj_id_4) as ccj4
        ,sum(ccj_id_5) as ccj5
        ,sum(ccj_id_1_2_3_4_5) as ccjall from bernie_nmarchio2.ut_gotv_dem_primary_eligible_vote_ready group by 1,2,3,4)));
commit;


