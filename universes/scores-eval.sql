set wlm_query_slot_count to 4;
DROP TABLE IF EXISTS bernie_nmarchio2.base_universe_qc;
CREATE TABLE bernie_nmarchio2.base_universe_qc
distkey(person_id) 
sortkey(person_id) as
(SELECT *, 
NTILE(100) OVER (PARTITION BY state_code ORDER BY gotv_validation_rank ASC) AS gotv_tiers_100,
NTILE(100) OVER (PARTITION BY state_code ORDER BY gotv_validation_rank_simple ASC) AS gotv_tiers_haystaq_100 
 from
 (select *

        ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1 then '0 - Donors and Activists'
        when biden_support_100 <= 90 
        or field_id_1_score_100 >= 70 
        or field_id_composite_score_100 >= 70 
        or current_support_raw_100 >= 70
        or sanders_strong_support_score_100 >= 70
        or spoke_persuasion_1minus_100 <= 70
        or spoke_support_1box_100 >= 80 
        or spoke_persuasion_1plus_100 >= 80 
        or attendee_100 >= 80 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as gotv_buckets

        ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1 
          then '0 - Donors and Activists'
        when biden_support_100 <= 90 
        or field_id_1_score_100 >= 70 
        or field_id_composite_score_100 >= 70 
        or current_support_raw_100 >= 70
        or sanders_strong_support_score_100 >= 70 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as gotv_buckets_simple

        ,round(1.0*count(*) OVER (partition BY state_code ORDER BY gotv_buckets ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
        ,row_number() OVER (PARTITION BY state_code ORDER BY gotv_buckets ASC, field_id_1_score DESC) as gotv_validation_rank

        ,round(1.0*count(*) OVER (partition BY state_code ORDER BY gotv_buckets_simple ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share_simple
        ,row_number() OVER (PARTITION BY state_code ORDER BY gotv_buckets_simple ASC, field_id_1_score DESC) as gotv_validation_rank_simple

from bernie_data_commons.base_universe where state_code in ('AL','AR','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA') ));


select *,
1.0*ccj1/ccj1all as ccj1rate,
1.0*ccj12/ccj1all as ccj12rate
from
(select
state_code,
 case --when rolling_electorate_share_simple <= .05 then '05%'
      when rolling_electorate_share_simple <= .10 then '10%'
      --when rolling_electorate_share_simple <= .15 then '15%'
      when rolling_electorate_share_simple <= .20 then '20%'
      --when rolling_electorate_share_simple <= .25 then '25%'
      when rolling_electorate_share_simple <= .30 then '30%'
      --when rolling_electorate_share_simple <= .35 then '35%'
      when rolling_electorate_share_simple <= .40 then '40%'
      --when rolling_electorate_share_simple <= .45 then '45%'
      when rolling_electorate_share_simple <= .50 then '50%'
      --when rolling_electorate_share_simple <= .55 then '55%'
      when rolling_electorate_share_simple <= .60 then '60%'
      --when rolling_electorate_share_simple <= .65 then '65%'
      when rolling_electorate_share_simple <= .70 then '70%'
      --when rolling_electorate_share_simple <= .75 then '75%'
      when rolling_electorate_share_simple <= .80 then '80%'
      --when rolling_electorate_share_simple <= .85 then '85%'
      when rolling_electorate_share_simple <= .90 then '90%'
      --when rolling_electorate_share_simple <= .95 then '95%'
 else 'Above 100%' end as rolling_share,
count(*) as number_of_voters,
sum(activist_flag) as activists,
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
bernie_nmarchio2.base_universe_qc group by 1,2) order by 1
