
set query_group to 'importers';
set wlm_query_slot_count to 3;

truncate bernie_data_commons.rainbow_analytics_frame; 
insert into bernie_data_commons.rainbow_analytics_frame
--drop table if exists bernie_data_commons.rainbow_analytics_frame; 
--create table bernie_data_commons.rainbow_analytics_frame
--distkey(person_id)
--sortkey(person_id)
--as 
(select

p.person_id
,p.reg_voter_flag
,p.state_code
,p.voting_address_latitude as v_latitude
,p.voting_address_longitude as v_longitude
,left(p.census_block_group_2010,5) as county_fips
,p.census_block_group_2010
,p.voting_city
    
,case 
when p.age_combined::int is not null and p.age_combined::int between 18 and 34 then '1 - 18-34'
when p.age_combined::int is not null and p.age_combined::int between 36 and 49 then '2 - 35-49'
when p.age_combined::int is not null and p.age_combined::int between 50 and 64 then '3 - 50-64'
when p.age_combined::int is not null and p.age_combined::int >= 65 then '4 - 65+'
when l2.voters_age::int is not null and l2.voters_age::int between 18 and 34 then '1 - 18-34'
when l2.voters_age::int is not null and l2.voters_age::int between 36 and 49 then '2 - 35-49'
when l2.voters_age::int is not null and l2.voters_age::int between 50 and 64 then '3 - 50-64'
when l2.voters_age::int is not null and l2.voters_age::int >= 65 then '4 - 65+'
when tc.xpg_ind_lvl_exact_age::int is not null and tc.xpg_ind_lvl_exact_age::int between 18 and 34 then '1 - 18-34'
when tc.xpg_ind_lvl_exact_age::int is not null and tc.xpg_ind_lvl_exact_age::int between 36 and 49 then '2 - 35-49'
when tc.xpg_ind_lvl_exact_age::int is not null and tc.xpg_ind_lvl_exact_age::int between 50 and 64 then '3 - 50-64'
when tc.xpg_ind_lvl_exact_age::int is not null and tc.xpg_ind_lvl_exact_age::int >= 65 then '4 - 65+'
when tc.xpg_ind_lvl_estimated_age::int is not null and tc.xpg_ind_lvl_estimated_age::int between 18 and 34 then '1 - 18-34'
when tc.xpg_ind_lvl_estimated_age::int is not null and tc.xpg_ind_lvl_estimated_age::int between 36 and 49 then '2 - 35-49'
when tc.xpg_ind_lvl_estimated_age::int is not null and tc.xpg_ind_lvl_estimated_age::int between 50 and 64 then '3 - 50-64'
when tc.xpg_ind_lvl_estimated_age::int is not null and tc.xpg_ind_lvl_estimated_age::int >= 65 then '4 - 65+'
else '5 - Unknown'
end as age_5way

,case
when tc.tb_education_cd in (3,4) then '1 - Bachelors or higher'
when tc.tb_education_cd in (1,2) then '2 - Less than Bachelors'
when as18.dnc_2018_college_graduate > .5 and tc.ts_tsmart_college_graduate_score > 50 then '1 - Bachelors or higher'
when as18.dnc_2018_college_graduate <= .5 and tc.ts_tsmart_high_school_only_score > 50 then '2 - Less than Bachelors'
when as18.dnc_2018_college_graduate > .5 then '1 - Bachelors or higher'
when as18.dnc_2018_college_graduate <= .5 then '2 - Less than Bachelors'
when tc.ts_tsmart_college_graduate_score > 50 then '1 - Bachelors or higher'
when tc.ts_tsmart_high_school_only_score > 50 then '2 - Less than Bachelors'
when right(tc.xpg_ind_lvl_education_model,1) in ('3','4') and left(tc.xpg_ind_lvl_education_model,1) in ('1','2') then  '1 - Bachelors or higher'
when right(tc.xpg_ind_lvl_education_model,1) in ('1','2') and left(tc.xpg_ind_lvl_education_model,1) in ('1','2') then '2 - Less than Bachelors'
when right(tc.xpg_ind_lvl_education_model,1) in ('3','4') and left(tc.xpg_ind_lvl_education_model,1) in  ('3','4','5') then '1 - Bachelors or higher'
when right(tc.xpg_ind_lvl_education_model,1) in ('1','2') and left(tc.xpg_ind_lvl_education_model,1) in ('3','4','5') then '2 - Less than Bachelors'
else '3 - Unknown'
end as education_2way

,case 
when bg.pct_college_acs_13_17 < 25 then '1 - Under 25% hold BAs'
when bg.pct_college_acs_13_17 >= 25 and bg.pct_college_acs_13_17 < 50 then '2 - 25-50% hold BAs'
when bg.pct_college_acs_13_17 >= 50 then '3 - 50%+ hold BAs'
else '4 - Unknown'
end as education_blockgroup_3way

,case
when tc.xpg_estimated_household_income in ('A','B') then '1 - Less than $25k'
when tc.xpg_estimated_household_income in ('C','D') then '2 - $25-50k'
when tc.xpg_estimated_household_income in ('E','F') then '3 - $50-100k'
when tc.xpg_estimated_household_income in ('G','H') then '4 - $100-150k'
when tc.xpg_estimated_household_income in ('I','J','K','L') then '5 - Over $150k'
when as18.dnc_2018_income_dollars < 25000 then '1 - Less than $25k'
when as18.dnc_2018_income_dollars >= 25000 and as18.dnc_2018_income_dollars < 50000 then '2 - $25-50k'
when as18.dnc_2018_income_dollars >= 50000 and as18.dnc_2018_income_dollars < 100000 then '3 - $50-100k'
when as18.dnc_2018_income_dollars >= 100000 and as18.dnc_2018_income_dollars < 150000 then '4 - $100-150k'
when as18.dnc_2018_income_dollars >= 150000 then '5 - Over $150k'
when bg.med_hhd_inc_bg_acs_13_17 < 25000 then '1 - Less than $25k'
when bg.med_hhd_inc_bg_acs_13_17 >= 25000 and bg.med_hhd_inc_bg_acs_13_17 < 50000 then '2 - $25-50k'
when bg.med_hhd_inc_bg_acs_13_17 >= 50000 and bg.med_hhd_inc_bg_acs_13_17 < 100000 then '3 - $50-100k'
when bg.med_hhd_inc_bg_acs_13_17 >= 100000 and bg.med_hhd_inc_bg_acs_13_17 < 150000 then '4 - $100-150k'
when bg.med_hhd_inc_bg_acs_13_17 >= 150000 then '5 - Over $150k'
else '6 - Unknown'
end as income_5way

,case 
when p.gender_combined = 'F' then '1 - Women'
when p.gender_combined = 'M' then '2 - Men'
when l2.voters_gender = 'F' then '1 - Women'
when l2.voters_gender = 'M' then '2 - Men'
when tc.tb_gender = 'F' then '1 - Women'
when tc.tb_gender = 'M' then '2 - Men'
when tc.xpg_ind_lvl_gender = 'F' then '1 - Women'
when tc.xpg_ind_lvl_gender = 'M' then '2 - Men'
else '3 - Unknown' end as gender_2way

,case 
when tc.ts_tsmart_urbanicity in ('U5', 'U6') then '1 - Urban'   
when tc.ts_tsmart_urbanicity in ('S3', 'S4') then '2 - Suburban'
when tc.ts_tsmart_urbanicity in ('R1', 'R2') then '3 - Rural'
when p.voting_address_urbanicity in ('U5','U6') then '1 - Urban'
when p.voting_address_urbanicity in ('S3','S4') then '2 - Suburban'
when p.voting_address_urbanicity in ('R1','R2') then '3 - Rural'  
when p.census_density_urban_area then '1 - Urban'
when p.census_density_urban_cluster then '2 - Suburban'
when p.census_density_rural then '3 - Rural'
else '4 - Unknown' end as urban_3way

,case 
when as20.civis_2020_likely_race = 'W' then '1 - White'
when as20.civis_2020_likely_race = 'B' then '2 - Black'
when as20.civis_2020_likely_race = 'H' then '3 - Latinx'
when as20.civis_2020_likely_race = 'A' then '4 - Asian'
when as20.civis_2020_likely_race = 'N' then '5 - Native'
when p.ethnicity_combined = 'W' then '1 - White'
when p.ethnicity_combined = 'B' then '2 - Black'
when p.ethnicity_combined = 'H' then '3 - Latinx'
when p.ethnicity_combined = 'A' then '4 - Asian'
when p.ethnicity_combined = 'N' then '5 - Native'
when tc.ts_tsmart_p_white >= .66 then '1 - White'
when tc.ts_tsmart_p_afam >= .66 then '2 - Black'
when tc.ts_tsmart_p_hisp >= .66 then '3 - Latinx'
when tc.ts_tsmart_p_asian >= .66 then '4 - Asian'
when tc.ts_tsmart_p_natam >= .66 then '5 - Native'
else '6 - Other' end as race_5way

,case
when greatest(as20.civis_2020_subeth_african_american,as20.civis_2020_subeth_west_indian,as20.civis_2020_subeth_haitian,as20.civis_2020_subeth_african,as20.civis_2020_subeth_other_black) = as20.civis_2020_subeth_african_american and as20.civis_2020_likely_race = 'B' then 'B1 - African American'
when greatest(as20.civis_2020_subeth_african_american,as20.civis_2020_subeth_west_indian,as20.civis_2020_subeth_haitian,as20.civis_2020_subeth_african,as20.civis_2020_subeth_other_black) = as20.civis_2020_subeth_west_indian and as20.civis_2020_likely_race = 'B' then 'B2 - West Indian'
when greatest(as20.civis_2020_subeth_african_american,as20.civis_2020_subeth_west_indian,as20.civis_2020_subeth_haitian,as20.civis_2020_subeth_african,as20.civis_2020_subeth_other_black) = as20.civis_2020_subeth_haitian and as20.civis_2020_likely_race = 'B' then 'B3 - Haitian'
when greatest(as20.civis_2020_subeth_african_american,as20.civis_2020_subeth_west_indian,as20.civis_2020_subeth_haitian,as20.civis_2020_subeth_african,as20.civis_2020_subeth_other_black) = as20.civis_2020_subeth_african and as20.civis_2020_likely_race = 'B' then 'B4 - African'
when greatest(as20.civis_2020_subeth_african_american,as20.civis_2020_subeth_west_indian,as20.civis_2020_subeth_haitian,as20.civis_2020_subeth_african,as20.civis_2020_subeth_other_black) = as20.civis_2020_subeth_other_black and as20.civis_2020_likely_race = 'B' then 'B5 - Other Black'

when greatest(as20.civis_2020_subeth_mexican,as20.civis_2020_subeth_cuban,as20.civis_2020_subeth_puerto_rican,as20.civis_2020_subeth_dominican,as20.civis_2020_subeth_other_latin_american,as20.civis_2020_subeth_other_hispanic) = as20.civis_2020_subeth_mexican and as20.civis_2020_likely_race = 'H' then 'L1 - Mexican'
when greatest(as20.civis_2020_subeth_mexican,as20.civis_2020_subeth_cuban,as20.civis_2020_subeth_puerto_rican,as20.civis_2020_subeth_dominican,as20.civis_2020_subeth_other_latin_american,as20.civis_2020_subeth_other_hispanic) = as20.civis_2020_subeth_cuban and as20.civis_2020_likely_race = 'H' then 'L2 - Cuban'
when greatest(as20.civis_2020_subeth_mexican,as20.civis_2020_subeth_cuban,as20.civis_2020_subeth_puerto_rican,as20.civis_2020_subeth_dominican,as20.civis_2020_subeth_other_latin_american,as20.civis_2020_subeth_other_hispanic) = as20.civis_2020_subeth_puerto_rican and as20.civis_2020_likely_race = 'H' then 'L3 - Puerto Rican'
when greatest(as20.civis_2020_subeth_mexican,as20.civis_2020_subeth_cuban,as20.civis_2020_subeth_puerto_rican,as20.civis_2020_subeth_dominican,as20.civis_2020_subeth_other_latin_american,as20.civis_2020_subeth_other_hispanic) = as20.civis_2020_subeth_dominican and as20.civis_2020_likely_race = 'H' then 'L4 - Dominican'
when greatest(as20.civis_2020_subeth_mexican,as20.civis_2020_subeth_cuban,as20.civis_2020_subeth_puerto_rican,as20.civis_2020_subeth_dominican,as20.civis_2020_subeth_other_latin_american,as20.civis_2020_subeth_other_hispanic) = as20.civis_2020_subeth_other_latin_american and as20.civis_2020_likely_race = 'H' then 'L5 - Other Latinx'
when greatest(as20.civis_2020_subeth_mexican,as20.civis_2020_subeth_cuban,as20.civis_2020_subeth_puerto_rican,as20.civis_2020_subeth_dominican,as20.civis_2020_subeth_other_latin_american,as20.civis_2020_subeth_other_hispanic) = as20.civis_2020_subeth_other_hispanic and as20.civis_2020_likely_race = 'H' then 'L5 - Other Latinx'

when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_chinese and as20.civis_2020_likely_race = 'A' then 'A1 - Chinese'
when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_indian and as20.civis_2020_likely_race = 'A' then 'A2 - Indian'
when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_filipino and as20.civis_2020_likely_race = 'A' then 'A3 - Filipino'
when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_japanese and as20.civis_2020_likely_race = 'A' then 'A4 - Japanese'
when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_vietnamese and as20.civis_2020_likely_race = 'A' then 'A5 - Vietnamese'
when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_korean and as20.civis_2020_likely_race = 'A' then 'A6 - Korean'
when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_other_asian and as20.civis_2020_likely_race = 'A' then 'A8 - Other Asian'
when greatest(as20.civis_2020_subeth_chinese,as20.civis_2020_subeth_indian,as20.civis_2020_subeth_filipino,as20.civis_2020_subeth_japanese,as20.civis_2020_subeth_vietnamese,as20.civis_2020_subeth_korean,as20.civis_2020_subeth_other_asian,as20.civis_2020_subeth_hmong) = as20.civis_2020_subeth_hmong and as20.civis_2020_likely_race = 'A' then 'A7 - Hmong'

when as20.civis_2020_likely_race = 'W' then 'W - White'
when as20.civis_2020_likely_race = 'B' then 'B5 - Other Black'
when as20.civis_2020_likely_race = 'H' then 'L5 - Other Latinx'
when as20.civis_2020_likely_race = 'A' then 'A8 - Other Asian'
when as20.civis_2020_likely_race = 'N' then 'N - Native'
when p.ethnicity_combined = 'W' then 'W - White'
when p.ethnicity_combined = 'B' then 'B5 - Other Black'
when p.ethnicity_combined = 'H' then 'L5 - Other Latinx'
when p.ethnicity_combined = 'A' then 'A5 - Other Asian'
when p.ethnicity_combined = 'N' then 'N - Native'
when tc.ts_tsmart_p_white >= .66 then 'W - White'
when tc.ts_tsmart_p_afam >= .66 then 'B5 - Other Black'
when tc.ts_tsmart_p_hisp >= .66 then 'L5 - Other Latinx'
when tc.ts_tsmart_p_asian >= .66 then 'A8 - Other Asian'
when tc.ts_tsmart_p_natam >= .66 then 'N - Native'
else 'O - Other' end as race_detailed_20way

,case 
when as20.civis_2020_spanish_language_preference >= .55 then '1 - Prefers Spanish' 
when as20.civis_2020_spanish_language_preference >= .5 and (acs.xc_16001_e_3 >= .2 or bg.pct_eng_vw_span_acs_13_17 >= .2) then '1 - Prefers Spanish' 
else '2 - Prefers English' end as spanish_language_2way

,case when as20.civis_2020_children_present > .3 then '1 - Child present'
else '2 - No child present' end as child_in_hh_2way

,case 
when as20.civis_2020_marriage > 0.66 or tc.ts_tsmart_marriage_score > 66 then '2 - Married'
when as20.civis_2020_marriage > 0.5 and tc.ts_tsmart_marriage_score > 50 and right(tc.xpg_ind_lvl_marital_status,1) = 'M' then '2 - Married'
else '1 - Single' end as marital_2way

,case
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_jewish and as20.civis_2020_cultural_religion_jewish >= 0.78 then '4 - Jewish'
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_mormon and as20.civis_2020_cultural_religion_mormon >= 0.78 then '6 - Mormon'
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_muslim and as20.civis_2020_cultural_religion_muslim >= 0.78 then '5 - Muslim'
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_catholic and as20.civis_2020_cultural_religion_catholic >= 0.78 then '3 - Catholic'
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_evangelical and as20.civis_2020_cultural_religion_evangelical >= 0.78 then '2 - Evangelical'
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_mainline_protestant and as20.civis_2020_cultural_religion_mainline_protestant >= 0.78 then '1 - Protestant'
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_hindu and as20.civis_2020_cultural_religion_hindu >= 0.78 then '7 - Hindu'
when greatest(as20.civis_2020_cultural_religion_jewish,as20.civis_2020_cultural_religion_mormon,as20.civis_2020_cultural_religion_muslim,as20.civis_2020_cultural_religion_catholic,as20.civis_2020_cultural_religion_evangelical,as20.civis_2020_cultural_religion_mainline_protestant,as20.civis_2020_cultural_religion_hindu,as20.civis_2020_cultural_religion_buddhist) = as20.civis_2020_cultural_religion_buddhist and as20.civis_2020_cultural_religion_buddhist >= 0.78 then '8 - Buddhist'
else '9 - Other' end as religion_9way

,case
when coalesce(p.party_name_dnc,l2.parties_description) = 'Democratic' or p.party_id = 1 then '1 - Democrat'
when coalesce(p.party_name_dnc,l2.parties_description) = 'Green' or p.party_id = 5 then '2 - Green'
when coalesce(p.party_name_dnc,l2.parties_description) = 'Independent' or p.party_id = 6 then '3 - Independent'
when coalesce(p.party_name_dnc,l2.parties_description) = 'Libertarian' or p.party_id = 4 then '4 - Libertarian'
when coalesce(p.party_name_dnc,l2.parties_description) = 'Republican' or p.party_id = 2 then '5 - Republican'
when coalesce(p.party_name_dnc,l2.parties_description) = 'Other' or p.party_id = 3 then '6 - Other'
when coalesce(p.party_name_dnc,l2.parties_description) = 'Other' or p.party_id = 7 then '6 - Other'
when coalesce(p.party_name_dnc,l2.parties_description) = 'Other' or p.party_id = 8 then '6 - Other'
when as20.civis_2020_partisanship >= .85 then '1 - Democrat'
when as20.civis_2020_partisanship >= .7 and as20.civis_2020_partisanship < .85 then '2 - Green'
when as20.civis_2020_partisanship >= .6 and as20.civis_2020_partisanship < .7 then '3 - Independent'
when as20.civis_2020_partisanship >= .4 and as20.civis_2020_partisanship < .6 then '4 - Libertarian'
when as20.civis_2020_partisanship < .4 then '5 - Republican'
else '6 - Other' end as party_6way

,case 
when as20.civis_2020_partisanship >= .66 then '1 - Democrat'
when as20.civis_2020_partisanship > .33 and as20.civis_2020_partisanship < .66 then '2 - Moderate'
when as20.civis_2020_partisanship <= 0.33 then '3 - Republican' 
when tc.ts_tsmart_partisan_score >= 66 then '1 - Democrat'
when tc.ts_tsmart_partisan_score > 33 and tc.ts_tsmart_partisan_score < 66 then '2 - Moderate'
when tc.ts_tsmart_partisan_score <= 33 then '3 - Republican' 
else '2 - Moderate' end as party_3way

,case
when as20.civis_2020_ideology_liberal >= .8 then '1 - Very liberal'
when as20.civis_2020_ideology_liberal >= .6 and as20.civis_2020_ideology_liberal < .8 then '2 - Somewhat liberal'
when as20.civis_2020_ideology_liberal >= .4 and as20.civis_2020_ideology_liberal < .6 then '3 - Centrist'
when as20.civis_2020_ideology_liberal >= .2 and as20.civis_2020_ideology_liberal < .4 then '4 - Somewhat conservative' 
when as20.civis_2020_ideology_liberal < .2 then '5 - Very conservative'
else '3 - Centrist' end as ideology_5way

,case 
when cty.primary16_sanders > cty.primary16_clinton then '1 - Sanders county'
else '2 - Clinton county' end as primary_vote_2016_2way

,case 
when p.registration_date::date > '2018-11-08' then '1 - Registered since 2018 election'
when (pv.vote_p_2008_party_d = 1 or
	  pv.vote_p_2009_party_d = 1 or
	  pv.vote_p_2010_party_d = 1 or
	  pv.vote_p_2011_party_d = 1 or
	  pv.vote_p_2012_party_d = 1 or
	  pv.vote_p_2013_party_d = 1 or
	  pv.vote_p_2014_party_d = 1 or
	  pv.vote_p_2015_party_d = 1 or
	  pv.vote_p_2016_party_d = 1 or
	  pv.vote_p_2017_party_d = 1 or
	  pv.vote_p_2018_party_d = 1 or 
	  pv.vote_pp_2000_party_d = 1 or
	  pv.vote_pp_2004_party_d = 1 or 
	  pv.vote_pp_2008_party_d = 1 or
	  pv.vote_pp_2016_party_d = 1) then '2 - Voted in a Dem Primary (2008-18)'
when (pv.vote_g_2008_novote_eligible = 1 or
	  pv.vote_g_2010_novote_eligible = 1 or 
	  pv.vote_g_2012_novote_eligible = 1 or
	  pv.vote_g_2014_novote_eligible = 1 or
	  pv.vote_g_2016_novote_eligible = 1 or
	  pv.vote_g_2018_novote_eligible = 1) then '3 - Eligible but did not vote in a General (2008-18)'
when (pv.vote_g_2018 = 1 or 
	  pv.vote_g_2016 = 1 or
	  pv.vote_g_2014 = 1 or
	  pv.vote_g_2012 = 1 or
	  pv.vote_g_2010 = 1 or
	  pv.vote_g_2008 = 1) then '4 - Voted only in a General (2008-18)'
else '5 - Other' end as vote_history_5way

,case 
when (pv.vote_pp_2016_method_early = 1 or
  pv.vote_p_2017_method_early = 1 or
  pv.vote_p_2018_method_early = 1) then '1 - Voted early in a Primary (2016-18)'
when (pv.vote_pp_2016 = 1 or
  pv.vote_p_2017 = 1 or
  pv.vote_p_2018 = 1) then '2 - Voted in a Primary (2016-18)'
when (pv.vote_g_2016 = 1 or
  pv.vote_g_2017 = 1 or
  pv.vote_g_2018 = 1) then '2 - Voted only in a General (2016-18)'
else '3 - Did not vote (2016-18)' end as early_voting_3way

,case 
when (pv.vote_pp_2008_method_absentee = 1 or
  pv.vote_pp_2012_method_absentee = 1 or
  pv.vote_pp_2016_method_absentee = 1) then '1 - Voted absentee in a Presidential Primary (2008-16)'
when (pv.vote_p_2014_method_absentee = 1 or
  pv.vote_p_2015_method_absentee = 1 or
  pv.vote_p_2016_method_absentee = 1 or
  pv.vote_p_2017_method_absentee = 1 or
  pv.vote_p_2018_method_absentee = 1) then '2 - Voted absentee in a Primary (2014-2018)'
when (pv.vote_g_2014_method_absentee = 1 or
  pv.vote_g_2015_method_absentee = 1 or
  pv.vote_g_2016_method_absentee = 1 or
  pv.vote_g_2017_method_absentee = 1 or
  pv.vote_g_2018_method_absentee = 1) then '3 - Voted absentee in a General (2014-2018)'
else '4 - Did not vote absentee (2014-18)' end as absentee_voting_4way

,case 
when greatest(
as18.civis_2018_cultural_persuasion,
as18.civis_2018_economic_persuasion,
as18.civis_2018_political_persuasion) = as18.civis_2018_economic_persuasion then '1 - Economic'
when greatest(
as18.civis_2018_cultural_persuasion,
as18.civis_2018_economic_persuasion,
as18.civis_2018_political_persuasion) = as18.civis_2018_cultural_persuasion then '2 - Cultural'
when greatest(
as18.civis_2018_cultural_persuasion,
as18.civis_2018_economic_persuasion,
as18.civis_2018_political_persuasion) = as18.civis_2018_political_persuasion then '3 - Political' 
else '4 - Undecided' end as fav_issue_area_3way

,case 
when greatest(
 as18.civis_2018_one_pct_persuasion
,as18.civis_2018_infrastructure_persuasion
,as18.civis_2018_aca_persuasion
,as18.civis_2018_skills_persuasion
,as18.civis_2018_college_persuasion
,as18.civis_2018_medicare_persuasion
,as18.civis_2018_progressive_persuasion)= as18.civis_2018_one_pct_persuasion then 'Working to strengthen the middle class and make sure the wealthiest 1% pay their fair share'
when greatest(
 as18.civis_2018_one_pct_persuasion
,as18.civis_2018_infrastructure_persuasion
,as18.civis_2018_aca_persuasion
,as18.civis_2018_skills_persuasion
,as18.civis_2018_college_persuasion
,as18.civis_2018_medicare_persuasion
,as18.civis_2018_progressive_persuasion)= as18.civis_2018_infrastructure_persuasion then 'Supporting federal investments to rebuild our infrastructure and put millions of Americans back to work in decent paying jobs in both the public and private sectors'
when greatest(
 as18.civis_2018_one_pct_persuasion
,as18.civis_2018_infrastructure_persuasion
,as18.civis_2018_aca_persuasion
,as18.civis_2018_skills_persuasion
,as18.civis_2018_college_persuasion
,as18.civis_2018_medicare_persuasion
,as18.civis_2018_progressive_persuasion)= as18.civis_2018_medicare_persuasion then 'Strengthening programs like Medicare and Social Security that ensure seniors can afford to live with dignity and obtain the health care they need'
when greatest(
 as18.civis_2018_one_pct_persuasion
,as18.civis_2018_infrastructure_persuasion
,as18.civis_2018_aca_persuasion
,as18.civis_2018_skills_persuasion
,as18.civis_2018_college_persuasion
,as18.civis_2018_medicare_persuasion
,as18.civis_2018_progressive_persuasion)= as18.civis_2018_skills_persuasion then 'Building an economy that gives working Americans the tools and skills to succeed in the 21st century'
when greatest(
 as18.civis_2018_one_pct_persuasion
,as18.civis_2018_infrastructure_persuasion
,as18.civis_2018_aca_persuasion
,as18.civis_2018_skills_persuasion
,as18.civis_2018_college_persuasion
,as18.civis_2018_medicare_persuasion
,as18.civis_2018_progressive_persuasion)= as18.civis_2018_college_persuasion then 'Making tuition at public colleges free for students from middle-class families'
when greatest(
 as18.civis_2018_one_pct_persuasion
,as18.civis_2018_infrastructure_persuasion
,as18.civis_2018_aca_persuasion
,as18.civis_2018_skills_persuasion
,as18.civis_2018_college_persuasion
,as18.civis_2018_medicare_persuasion
,as18.civis_2018_progressive_persuasion)= as18.civis_2018_aca_persuasion then 'Protecting the Affordable Care Act and expanding Medicaid to ensure Americans have access to high-quality health care at an affordable price'
when greatest(
 as18.civis_2018_one_pct_persuasion
,as18.civis_2018_infrastructure_persuasion
,as18.civis_2018_aca_persuasion
,as18.civis_2018_skills_persuasion
,as18.civis_2018_college_persuasion
,as18.civis_2018_medicare_persuasion
,as18.civis_2018_progressive_persuasion)= as18.civis_2018_progressive_persuasion then 'Working to advance a progressive agenda in Washington' 
else 'Undecided' end as fav_econ_issue_7way

,case 
when greatest(
 as18.civis_2018_growth_persuasion
,as18.civis_2018_bipartisan_persuasion
,as18.civis_2018_welcome_persuasion
,as18.civis_2018_trump_persuasion
,as18.civis_2018_gop_persuasion) = as18.civis_2018_bipartisan_persuasion then 'Working with members of both parties to improve the lives of all Americans'
when greatest(
 as18.civis_2018_growth_persuasion
,as18.civis_2018_bipartisan_persuasion
,as18.civis_2018_welcome_persuasion
,as18.civis_2018_trump_persuasion
,as18.civis_2018_gop_persuasion) = as18.civis_2018_gop_persuasion then 'Standing up to Congressional Republicans policies that benefit the rich'
when greatest(
 as18.civis_2018_growth_persuasion
,as18.civis_2018_bipartisan_persuasion
,as18.civis_2018_welcome_persuasion
,as18.civis_2018_trump_persuasion
,as18.civis_2018_gop_persuasion) = as18.civis_2018_growth_persuasion then 'Working to promote growth and build an economic engine that creates jobs'
when greatest(
 as18.civis_2018_growth_persuasion
,as18.civis_2018_bipartisan_persuasion
,as18.civis_2018_welcome_persuasion
,as18.civis_2018_trump_persuasion
,as18.civis_2018_gop_persuasion) = as18.civis_2018_trump_persuasion then 'Standing up to Donald Trumps toxic politics'
when greatest(
 as18.civis_2018_growth_persuasion
,as18.civis_2018_bipartisan_persuasion
,as18.civis_2018_welcome_persuasion
,as18.civis_2018_trump_persuasion
,as18.civis_2018_gop_persuasion) = as18.civis_2018_welcome_persuasion then 'Working to build an America where people of all backgrounds are welcomed' 
else 'Undecided' end as fav_poli_issue_5way

,case
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_choice_persuasion then 'Ensuring every woman has access to safe and legal abortion services'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_sexual_assault_persuasion then 'Fighting to prevent sexual assault and ensure that sexual abusers face consequences'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_wall_persuasion then 'Fighting against an unnecessary and divisive wall on the border with Mexico'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_marijuana_persuasion then 'Legalizing the use of recreational marijuana'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_race_persuasion then 'Protecting the civil rights of racial and ethnic minorities and fighting to end discrimination'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_climate_persuasion then 'Working to protect the environment against climate change while expanding the clean energy economy'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_lgbt_persuasion then 'Protection the civil rights of LGBT Americans and fighting to end discrimination'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_guns_persuasion then 'Support common sense gun safety measures like universal background checks for gun sales and banning the sale of high capacity ammunition cartridges for semi-automatic weapons'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_dreamers_persuasion then 'Fighting to protect DREAMers,immigrant children who were brought to this country illegally as children'
when greatest(
 as18.civis_2018_climate_persuasion
,as18.civis_2018_sexual_assault_persuasion
,as18.civis_2018_wall_persuasion
,as18.civis_2018_marijuana_persuasion
,as18.civis_2018_race_persuasion
,as18.civis_2018_lgbt_persuasion
,as18.civis_2018_guns_persuasion
,as18.civis_2018_dreamers_persuasion
,as18.civis_2018_military_persuasion
,as18.civis_2018_choice_persuasion)  = as18.civis_2018_military_persuasion then 'Investing in the military and defense to keep America safe from terrorism'
else 'Undecided' end as fav_cultural_issue_10way

from phoenix_analytics.person p 
left join phoenix_analytics.person_votes pv using(person_id)
left join phoenix_scores.all_scores_2020 as20 using(person_id) 
left join phoenix_scores.all_scores_2018 as18 using(person_id) 
left join phoenix_scores.all_scores_2016 as16 using(person_id) 
left join phoenix_consumer.tsmart_consumer tc using(person_id) 
left join bernie_nmarchio2.geo_county_covariates cty on left(p.census_block_group_2010,5) = lpad(cty.county_fip_id,5,'00000')
left join bernie_nmarchio2.geo_tract_covariates trct on left(p.census_block_group_2010,11) = lpad(trct.tract_id,11,'00000000000')
left join bernie_nmarchio2.geo_block_covariates bg on p.census_block_group_2010 = lpad(bg.block_group_id, 12,'000000000000') 
left join phoenix_census.acs_current acs on p.census_block_group_2010 = acs.block_group_id 
left join bernie_data_commons.master_xwalk_dnc x using(person_id)
left join l2.demographics l2 using(lalvoterid)

where p.is_deceased = false -- is alive
and p.reg_record_merged = false -- removes duplicated registration addresses
and p.reg_on_current_file = true --  voter was on the last voter file that the DNC processed and not eligible to vote in primaries
and p.reg_voter_flag = true -- voters who are registered to vote (i.e. have a registration status of active or inactive) even if they have moved states and their new state has not updated their file to reflect this yet
--and p.state_code in ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA'));

grant select on table bernie_data_commons.rainbow_analytics_frame to group bernie_data;
