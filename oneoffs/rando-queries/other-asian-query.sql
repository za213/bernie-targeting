select * from
(select person_id, first_name, last_name, census_block_group_2010, state_code from phoenix_analytics.person where is_deceased = false
and reg_record_merged = false
and reg_on_current_file = true 
and reg_voter_flag = true) p 
left join 
(select person_id, civis_2020_subeth_other_asian, civis_2020_subeth_hmong, civis_2020_subeth_indian, civis_2020_cultural_religion_hindu, civis_2020_cultural_religion_buddhist from phoenix_scores.all_scores_2020) as20 using(person_id) 
left join
(select person_id , race_detailed_20way, religion_9way from bernie_data_commons.rainbow_analytics_frame) rainbo using(person_id)
left join 
(select block_group_id, xc_16001_e_30, xb_05007_e_15, xc_16001_e_30 from phoenix_census.acs_current) acs on p.census_block_group_2010 = acs.block_group_id 
where 
civis_2020_subeth_other_asian > .78 or 
civis_2020_subeth_hmong > .78 or 
civis_2020_subeth_indian > .78 or
civis_2020_cultural_religion_hindu > .78 or 
civis_2020_cultural_religion_buddhist > .78 -- or 
--(xc_16001_e_30 > 20 or xb_05007_e_15 > 20 or xc_16001_e_30 > 20)
