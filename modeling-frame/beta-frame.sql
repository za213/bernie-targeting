
set query_group to 'importers';

drop table if exists bernie_data_commons.phoenix_modeling_frame; 
create table bernie_data_commons.phoenix_modeling_frame 
distkey(person_id)
sortkey(person_id)
as (
  
select p.person_id
  ,x.jsonid
  
  ,coalesce(pri.primary16_clinton,.5) as primary16_clinton
  ,coalesce(pri.primary16_sanders,.5) as primary16_sanders
  ,coalesce(dnc_2020_volprop_overall_rank ,0.5) as dnc_2020_volprop_overall_rank
  ,coalesce(civis_2020_race_native ,0.5) as civis_2020_race_native
  ,coalesce(civis_2020_race_black ,0.5) as civis_2020_race_black
  ,coalesce(civis_2020_race_latinx ,0.5) as civis_2020_race_latinx
  ,coalesce(civis_2020_race_asian ,0.5) as civis_2020_race_asian
  ,coalesce(civis_2020_race_white ,0.5) as civis_2020_race_white
  ,coalesce(civis_2020_likely_race ,0.5) as civis_2020_likely_race
  ,coalesce(civis_2020_likely_race_confidence ,0.5) as civis_2020_likely_race_confidence
  ,coalesce(civis_2020_subeth_african_american ,0.5) as civis_2020_subeth_african_american
  ,coalesce(civis_2020_subeth_west_indian ,0.5) as civis_2020_subeth_west_indian
  ,coalesce(civis_2020_subeth_haitian ,0.5) as civis_2020_subeth_haitian
  ,coalesce(civis_2020_subeth_african ,0.5) as civis_2020_subeth_african
  ,coalesce(civis_2020_subeth_other_black ,0.5) as civis_2020_subeth_other_black
  ,coalesce(civis_2020_subeth_mexican ,0.5) as civis_2020_subeth_mexican
  ,coalesce(civis_2020_subeth_cuban ,0.5) as civis_2020_subeth_cuban
  ,coalesce(civis_2020_subeth_puerto_rican ,0.5) as civis_2020_subeth_puerto_rican
  ,coalesce(civis_2020_subeth_dominican ,0.5) as civis_2020_subeth_dominican
  ,coalesce(civis_2020_subeth_other_latin_american ,0.5) as civis_2020_subeth_other_latin_american
  ,coalesce(civis_2020_subeth_other_hispanic ,0.5) as civis_2020_subeth_other_hispanic
  ,coalesce(civis_2020_subeth_chinese ,0.5) as civis_2020_subeth_chinese
  ,coalesce(civis_2020_subeth_indian ,0.5) as civis_2020_subeth_indian
  ,coalesce(civis_2020_subeth_filipino ,0.5) as civis_2020_subeth_filipino
  ,coalesce(civis_2020_subeth_japanese ,0.5) as civis_2020_subeth_japanese
  ,coalesce(civis_2020_subeth_vietnamese ,0.5) as civis_2020_subeth_vietnamese
  ,coalesce(civis_2020_subeth_korean ,0.5) as civis_2020_subeth_korean
  ,coalesce(civis_2020_subeth_other_asian ,0.5) as civis_2020_subeth_other_asian
  ,coalesce(civis_2020_subeth_hmong ,0.5) as civis_2020_subeth_hmong
  ,coalesce(civis_2020_partisanship ,0.5) as civis_2020_partisanship
  ,coalesce(civis_2020_ideology_liberal ,0.5) as civis_2020_ideology_liberal
  ,coalesce(civis_2020_spanish_language_preference ,0.5) as civis_2020_spanish_language_preference
  ,coalesce(civis_2020_cultural_religion_jewish ,0.5) as civis_2020_cultural_religion_jewish
  ,coalesce(civis_2020_cultural_religion_mormon ,0.5) as civis_2020_cultural_religion_mormon
  ,coalesce(civis_2020_cultural_religion_muslim ,0.5) as civis_2020_cultural_religion_muslim
  ,coalesce(civis_2020_cultural_religion_catholic ,0.5) as civis_2020_cultural_religion_catholic
  ,coalesce(civis_2020_cultural_religion_evangelical ,0.5) as civis_2020_cultural_religion_evangelical
  ,coalesce(civis_2020_cultural_religion_mainline_protestant ,0.5) as civis_2020_cultural_religion_mainline_protestant
  ,coalesce(civis_2020_cultural_religion_hindu ,0.5) as civis_2020_cultural_religion_hindu
  ,coalesce(civis_2020_cultural_religion_buddhist ,0.5) as civis_2020_cultural_religion_buddhist
  ,coalesce(civis_2020_marriage ,0.5) as civis_2020_marriage
  ,coalesce(civis_2020_children_present ,0.5) as civis_2020_children_present

-- Urbanity 
  ,case 
    when tc.ts_tsmart_urbanicity in ('R1', 'R2') 
    then 1 else 0                 end as geo_rural
  ,case  
    when tc.ts_tsmart_urbanicity in ('S3', 'S4') 
    then 1 else 0                 end as geo_suburban
  ,case 
    when tc.ts_tsmart_urbanicity in ('U5', 'U6') 
    then 1 else 0                 end as geo_urban
  
-- Gender
  ,case 
    when coalesce(p.gender_combined,l2.voters_gender)  = 'F' 
    then 1 else 0                 end as female 
  
-- Ethnicity
  ,case 
    when p.ethnicity_combined = 'W' 
    or ((ethnicity_combined is null or ethnicity_combined ='O')
        and(civis_2020_race_white > civis_2020_race_native
        and civis_2020_race_white > civis_2020_race_black
        and civis_2020_race_white > civis_2020_race_asian
        and civis_2020_race_white > civis_2020_race_latinx)) then 1 else 0 end as eth_white
  
  ,case 
    when p.ethnicity_combined = 'A' 
    or ((ethnicity_combined is null or ethnicity_combined ='O')
        and(civis_2020_race_asian > civis_2020_race_native
        and civis_2020_race_asian > civis_2020_race_black
        and civis_2020_race_asian > civis_2020_race_white
        and civis_2020_race_asian > civis_2020_race_latinx)) then 1 else 0 end as eth_asian

  ,case 
    when p.ethnicity_combined = 'B' 
    or ((ethnicity_combined is null or ethnicity_combined ='O')
        and(civis_2020_race_black > civis_2020_race_native
        and civis_2020_race_black > civis_2020_race_latinx
        and civis_2020_race_black > civis_2020_race_asian
        and civis_2020_race_black > civis_2020_race_white)) then 1 else 0 end as eth_afam
  
  ,case 
    when p.ethnicity_combined = 'H' 
    or ((ethnicity_combined is null or ethnicity_combined = 'O')
        and(civis_2020_race_latinx > civis_2020_race_native
        and civis_2020_race_latinx  > civis_2020_race_black
        and civis_2020_race_latinx  > civis_2020_race_asian
        and civis_2020_race_latinx  > civis_2020_race_white)) then 1 else 0 end as eth_hispanic

  ,case 
    when p.ethnicity_combined = 'N' 
    or ((ethnicity_combined is null or ethnicity_combined = 'O')
        and (civis_2020_race_native > civis_2020_race_black
        and civis_2020_race_native  > civis_2020_race_latinx
        and civis_2020_race_native  > civis_2020_race_asian
        and civis_2020_race_native  > civis_2020_race_white)) then 1 else 0 end as eth_native

-- Religion 
  ,case 
    when l2.religions_description = 'Islamic' 
    then 1 else 0 end as religion_muslim 

-- Income 
  ,case 
    when tc.xpg_estimated_household_income =  'A' then 1 else 0 end as inc_1k_14k
  ,case 
    when tc.xpg_estimated_household_income =  'B' then 1 else 0 end as inc_15k_24k
  ,case 
    when tc.xpg_estimated_household_income =  'C' then 1 else 0 end as inc_25k_34k
  ,case 
    when tc.xpg_estimated_household_income =  'D' then 1 else 0 end as inc_35k_49k
  ,case 
    when tc.xpg_estimated_household_income =  'E' then 1 else 0 end as inc_50k_74k
  ,case 
    when tc.xpg_estimated_household_income =  'F' then 1 else 0 end as inc_75k_99k
  ,case 
    when tc.xpg_estimated_household_income =  'G' then 1 else 0 end as inc_100k_124k
  ,case 
    when tc.xpg_estimated_household_income =  'H' then 1 else 0 end as inc_125k_149k
  ,case 
    when tc.xpg_estimated_household_income =  'I' then 1 else 0 end as inc_150k_174k
  ,case 
    when tc.xpg_estimated_household_income =  'J' then 1 else 0 end as inc_175k_199k
  ,case 
    when tc.xpg_estimated_household_income =  'K' then 1 else 0 end as inc_200k_249k
  ,case 
    when tc.xpg_estimated_household_income =  'L' then 1 else 0 end as inc_250k

-- Age     
  ,coalesce(p.age_combined,l2.voters_age::int) as age_continuous
  
  ,case 
    when coalesce(p.age_combined,l2.voters_age::int) between 18 and 22
        then 1 else 0 end as age_18_22
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int) between 23 and 29
        then 1 else 0 end as age_23_29
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int) between 30 and 39
        then 1 else 0 end as age_30_39
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int) between 40 and 49
        then 1 else 0 end as age_40_49
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int) between 50 and 59
        then 1 else 0 end as age_50_59
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int) between 60 and 69
        then 1 else 0 end as age_60_69
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int) between 70 and 79
        then 1 else 0 end as age_70_79
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int)  >= 80
        then 1 else 0 end as age_80plus
    
-- Education 
  ,case
    when tc.tb_education_cd = 0 then 1 else 0 end as less_than_HS       
  ,case
    when tc.tb_education_cd = 1 then 1 else 0 end as edu_HS      
  ,case
    when tc.tb_education_cd = 2 then 1 else 0 end as edu_some_college
  ,case
    when tc.tb_education_cd = 3 then 1 else 0 end as edu_bachelors
  ,case 
    when tc.tb_education_cd = 4 then 1 else 0 end as edu_grad_degree
    
-- Party ID 
  ,case
    when coalesce(p.party_name_dnc,l2.parties_description) = 'Democratic' then 1 else 0 end as party_dem
  ,case  
    when coalesce(p.party_name_dnc,l2.parties_description) = 'Republican' then 1 else 0 end as party_rep
  ,case  
    when coalesce(p.party_name_dnc,l2.parties_description) = 'Other' then 1 else 0 end as party_other

-- Voter history
  ,case
    when vote_g_2018 = 1 and vote_g_2016 != 1
    and vote_g_2014 != 1 or vote_g_2012 != 1 
    then 1 else 0 end as vote_midterm_voter
  ,case  
    when vote_g_2018 = 1 and vote_g_2016 != 1
    and vote_g_2014 != 1 or vote_g_2012 != 1  
    then 1 else 0 end as vote_presidential_voter
  ,case  
    when vote_g_2018 != 1 and vote_g_2016 != 1
    and (vote_g_2014 = 1 or vote_g_2012 = 1)  
    then 1 else 0 end as vote_lapsed_voter
  ,case  
    when registration_date::date > '2018-11-08' then 1 else 0 end as vote_new_reg
  
from phoenix_analytics.person p 
left join phoenix_analytics.person_votes pv using(person_id)
left join phoenix_scores.all_scores_2020 score using(person_id) 
left join phoenix_consumer.tsmart_consumer tc using(person_id) 

left join bernie_data_commons.master_xwalk_dnc x using(person_id)
left join l2.demographics l2 using(lalvoterid)

left join bernie_nmarchio2.primaryreturns16 pri on p.county_fips = right(census_county_fips,'3') and p.state_fips = left(lpad(census_county_fips,5,'000'),2)

  where p.is_deceased = false -- is alive
  and p.reg_record_merged = false -- removes some duplicates
  and p.reg_on_current_file = true --  voter was on the last voter file that the DNC processed and not eligible to vote in primaries
  and p.reg_voter_flag = true -- voters who are registered to vote (i.e. have a registration status of active or inactive) even if they have moved states and their new state has not updated their file to reflect this yet

);

grant select on table bernie_data_commons.phoenix_modeling_frame to group bernie_data;
