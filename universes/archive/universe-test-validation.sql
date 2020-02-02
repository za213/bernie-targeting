
set wlm_query_slot_count to 4;
DROP TABLE IF EXISTS bernie_nmarchio2.base_universe_qc;
CREATE TABLE bernie_nmarchio2.base_universe_qc
distkey(person_id) 
sortkey(person_id) as
(SELECT *, 
NTILE(100) OVER (PARTITION BY state_code ORDER BY support_tier_validation_rank ASC) AS gotv_tiers_100,
NTILE(100) OVER (PARTITION BY state_code ORDER BY support_tier_validation_haystaq_rank ASC) AS gotv_tiers_haystaq_100 
 from
 (select *

        ,case 
           when electorate_2way = '2 - Non-target' THEN '5 - Non-target' 
           when current_support_raw_100 >= 90 or field_id_1_score_100 >= 90 or field_id_composite_score_100 >= 90 then '1 - Support Tier 1'
           when current_support_raw_100 >= 80 or field_id_1_score_100 >= 80 or field_id_composite_score_100 >= 80 then '2 - Support Tier 2'
           when current_support_raw_100 >= 70 or field_id_1_score_100 >= 70 or field_id_composite_score_100 >= 70  then '3 - Support Tier 3'
        else '4 - Support Tier 4' end as score_thresholds_haystaq

        ,CASE 
        WHEN score_thresholds_haystaq = '5 - Non-target' then '4 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1 then '0 - Donors and Activists'
        WHEN score_thresholds_haystaq = '1 - Support Tier 1' then '1 - Support Tier 1'
        WHEN score_thresholds_haystaq = '2 - Support Tier 2' then '2 - Support Tier 2'
        WHEN score_thresholds_haystaq = '3 - Support Tier 3' then '3 - Support Tier 3'
        ELSE '4 - Non-target' END AS support_tier_validation_haystaq

        ,CASE 
        WHEN score_thresholds_haystaq = '5 - Non-target' 
          or ccj_id_5 = 1 
          or thirdp_support_5_id = 1 
          or thirdp_first_choice_biden_warren_buttigieg = 1 then '4 - Non-target' 
        WHEN ccj_id_1 = 1 
          or thirdp_first_choice_bernie = 1 
          or thirdp_support_1_id = 1 
          or support_tier_validation = '0 - Donors and Activists' then '0 - Donors, Activists, Support 1 IDs'
        WHEN score_thresholds_haystaq = '1 - Support Tier 1' then '1 - Support Tier 1'
        WHEN score_thresholds_haystaq = '2 - Support Tier 2' then '2 - Support Tier 2'
        WHEN score_thresholds_haystaq = '3 - Support Tier 3' or ccj_id_2 = 1 or thirdp_support_2_id = 1 then '3 - Support Tier 3'
        ELSE '4 - Non-target' END AS support_tier_juiced_haystaq

        ,round(1.0*count(*) OVER (partition BY state_code ORDER BY electorate_2way ASC,support_tier_validation ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
        ,row_number() OVER (PARTITION BY state_code ORDER BY support_tier_validation ASC, field_id_1_score DESC) as support_tier_validation_rank

        ,round(1.0*count(*) OVER (partition BY state_code ORDER BY electorate_2way ASC,support_tier_validation_haystaq ASC, field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share_haystaq
        ,row_number() OVER (PARTITION BY state_code ORDER BY support_tier_validation_haystaq ASC, field_id_1_score DESC) as support_tier_validation_haystaq_rank

from
bernie_data_commons.base_universe where electorate_2way = '1 - Target universe' and state_code in ('AL','AR','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA') ));
