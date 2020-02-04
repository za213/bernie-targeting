select * from student_data.school_formatted_229018_university_of_texas_permian_basin


-- Standard Recipe
set wlm_query_slot_count to 3;
DROP TABLE IF EXISTS bernie_jmeadow.base_universe_qc_0204;
CREATE TABLE bernie_jmeadow.base_universe_qc_0204
distkey(person_id)
sortkey(person_id) as
(SELECT person_id
,state_code
,ccj_holdout_group
,activist_flag
,activist_household_flag
,donor_1plus_flag
,donor_1plus_flag_household_flag
,ccj_id_1
,ccj_id_2
,ccj_id_3
,ccj_id_4
,ccj_id_5
,ccj_id_1_2_3_4_5
,ccj_id_1_2
,field_id_1_score_100
,current_support_raw_100
,electorate_2way
,NTILE(20) OVER (PARTITION BY state_code ORDER BY gotv_validation_rank ASC) AS gotv_tiers_20
 from
 (select *
        ,round(1.0*count(*) OVER (partition BY state_code ORDER BY field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
        ,row_number() OVER (PARTITION BY state_code ORDER BY field_id_1_score DESC) as gotv_validation_rank
from bernie_data_commons.base_universe where state_code in ('AL','AR','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA')
) x

);


-- Scores Eval
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
bernie_jmeadow.base_universe_qc_0204 where electorate_2way = '1 - Target universe'  group by 1,2,3) order by 1,2,3
