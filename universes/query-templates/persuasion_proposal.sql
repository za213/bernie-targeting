DROP TABLE IF EXISTS bernie_aschang.persuasion_base_universes;
CREATE TABLE bernie_aschang.persuasion_base_universes
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code||dem_primary_eligible_2way ORDER BY pers_rank ASC) AS pers_tiers_20
 from
 (select  person_id
         ,state_code
         ,dem_primary_eligible_2way
         ,support_guardrail
         ,case when 
  			(age_5way != '1 - 18-34' and race_5way = '2 - Black')
  				or spoke_persuasion_1plus_100 > 70 
  				or sanders_very_excited_score_100 > 80
  				or  ((biden_support_100 > 80 or warren_support_100 > 80) AND buttigieg_support_100 < 90) 
  				then 1 else 0 end as persuasion_target

  -- remove non-electorate, anyone who is a 1, or anyone who is a 5, or anyone who is a strong supporter already 
  -- Sort Persuasion Targets by turnout for the tiering
        ,CASE WHEN electorate_2way = '2 - Non-target' 
  				or ccj_id_5 = 1 
  				or ccj_id_5_hh = 1 
  				or activist_flag = 1
  				or donor_1plus_flag = 1 
  				or ccj_id_1 = 1
  				or thirdp_support_1_id = 1
  				or thirdp_first_choice_bernie = 1
  				or field_id_1_score_100 > 90
  				or field_id_composite_score_100 > 90
  				or sanders_strong_support_score_100 > 90
  				then '6 - Non-target' 
 		WHEN  turnout_current_100 > 80 
  			AND persuasion_target = 1
  			then '1 - Highest Turnout Persuasion Targets'
 		WHEN  turnout_current_100 > 60 
  			AND persuasion_target = 1 then '2 - High Turnout Persuasion Targets'
 		WHEN  turnout_current_100 > 50 
  			AND persuasion_target = 1 then '3 - Lower Turnout Persuasion Targets'
  		WHEN  turnout_current_100 > 20 
  				and persuasion_target = 1 then '4 - Low Turnout Persuasion Targets'
  		WHEN spoke_persuasion_1plus_100 > 50 then  '5 - Low Persuasion Targets'
  		ELSE '6 - Non-target' end as custom_tiers
                
    -- REMOVE 5s and 1s from non-target so that we can see validation across tiers. 
  		,CASE WHEN electorate_2way = '2 - Non-target' 
  				or activist_flag = 1
  				or donor_1plus_flag = 1 
  				or field_id_1_score_100 > 90
  				or field_id_composite_score_100 > 90
  				or sanders_strong_support_score_100 > 90
  				then '6 - Non-target' 
 		WHEN  turnout_current_100 > 80 
  			AND persuasion_target = 1
  			then '1 - Highest Turnout Persuasion Targets'
 		WHEN  turnout_current_100 > 60 
  			AND persuasion_target = 1 then '2 - High Turnout Persuasion Targets'
 		WHEN  turnout_current_100 > 50 
  			AND persuasion_target = 1 then '3 - Lower Turnout Persuasion Targets'
  		WHEN  turnout_current_100 > 20 
  				and persuasion_target = 1 then '4 - Low Turnout Persuasion Targets'
  		WHEN spoke_persuasion_1plus_100 > 50 then  '5 - Low Persuasion Targets'
  		ELSE '6 - Non-target' end as custom_tiers_validation
         ,round(1.0*count(*) OVER (partition BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, spoke_persuasion_1plus_100 DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
         ,row_number() OVER (PARTITION BY state_code||dem_primary_eligible_2way  ORDER BY custom_tiers ASC, spoke_persuasion_1plus_100 DESC) as pers_rank
from bernie_data_commons.base_universe 
where dem_primary_eligible_2way  = '1 - Dem Primary Eligible' 
and (civis_2020_partisanship >= .33 or party_8way in ('1 - Democratic', '3 - Independent', '5 - Unaffiliated', '4 - Nonpartisan'))
));
