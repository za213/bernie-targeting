
create temp table all_scores_impute as (
select 
census_block_group_2010
,avg(civis_2020_race_native) as avg_civis_2020_race_native
,avg(civis_2020_race_black) as avg_civis_2020_race_black
,avg(civis_2020_race_latinx) as avg_civis_2020_race_latinx
,avg(civis_2020_race_asian) as avg_civis_2020_race_asian
,avg(civis_2020_race_white) as avg_civis_2020_race_white
,avg(civis_2020_likely_race) as avg_civis_2020_likely_race
,avg(civis_2020_likely_race_confidence) as avg_civis_2020_likely_race_confidence
,avg(civis_2020_subeth_african_american) as avg_civis_2020_subeth_african_american
,avg(civis_2020_subeth_west_indian) as avg_civis_2020_subeth_west_indian
,avg(civis_2020_subeth_haitian) as avg_civis_2020_subeth_haitian
,avg(civis_2020_subeth_african) as avg_civis_2020_subeth_african
,avg(civis_2020_subeth_other_black) as avg_civis_2020_subeth_other_black
,avg(civis_2020_subeth_mexican) as avg_civis_2020_subeth_mexican
,avg(civis_2020_subeth_cuban) as avg_civis_2020_subeth_cuban
,avg(civis_2020_subeth_puerto_rican) as avg_civis_2020_subeth_puerto_rican
,avg(civis_2020_subeth_dominican) as avg_civis_2020_subeth_dominican
,avg(civis_2020_subeth_other_latin_american) as avg_civis_2020_subeth_other_latin_american
,avg(civis_2020_subeth_other_hispanic) as avg_civis_2020_subeth_other_hispanic
,avg(civis_2020_subeth_chinese) as avg_civis_2020_subeth_chinese
,avg(civis_2020_subeth_indian) as avg_civis_2020_subeth_indian
,avg(civis_2020_subeth_filipino) as avg_civis_2020_subeth_filipino
,avg(civis_2020_subeth_japanese) as avg_civis_2020_subeth_japanese
,avg(civis_2020_subeth_vietnamese) as avg_civis_2020_subeth_vietnamese
,avg(civis_2020_subeth_korean) as avg_civis_2020_subeth_korean
,avg(civis_2020_subeth_other_asian) as avg_civis_2020_subeth_other_asian
,avg(civis_2020_subeth_hmong) as avg_civis_2020_subeth_hmong
,avg(civis_2020_partisanship) as avg_civis_2020_partisanship
,avg(civis_2020_ideology_liberal) as avg_civis_2020_ideology_liberal
,avg(civis_2020_spanish_language_preference) as avg_civis_2020_spanish_language_preference
,avg(civis_2020_cultural_religion_jewish) as avg_civis_2020_cultural_religion_jewish
,avg(civis_2020_cultural_religion_mormon) as avg_civis_2020_cultural_religion_mormon
,avg(civis_2020_cultural_religion_muslim) as avg_civis_2020_cultural_religion_muslim
,avg(civis_2020_cultural_religion_catholic) as avg_civis_2020_cultural_religion_catholic
,avg(civis_2020_cultural_religion_evangelical) as avg_civis_2020_cultural_religion_evangelical
,avg(civis_2020_cultural_religion_mainline_protestant) as avg_civis_2020_cultural_religion_mainline_protestant
,avg(civis_2020_cultural_religion_hindu) as avg_civis_2020_cultural_religion_hindu
,avg(civis_2020_cultural_religion_buddhist) as avg_civis_2020_cultural_religion_buddhist
,avg(civis_2020_marriage) as avg_civis_2020_marriage
,avg(civis_2020_children_present) as avg_civis_2020_children_present
,avg(dnc_2018_college_graduate) as avg_dnc_2018_college_graduate
,avg(dnc_2018_marriage) as avg_dnc_2018_marriage
,avg(dnc_2018_income_dollars) as avg_dnc_2018_income_dollars
,avg(dnc_2018_downballot_defection_rank) as avg_dnc_2018_downballot_defection_rank
,avg(dnc_2018_downballot_vote) as avg_dnc_2018_downballot_vote
,avg(dnc_2018_ideology_liberal) as avg_dnc_2018_ideology_liberal
,avg(dnc_2018_ideology_moderate) as avg_dnc_2018_ideology_moderate
,avg(dnc_2018_ideology_conservative) as avg_dnc_2018_ideology_conservative
,avg(dnc_2018_campaign_finance) as avg_dnc_2018_campaign_finance
,avg(dnc_2018_climate_change) as avg_dnc_2018_climate_change
,avg(dnc_2018_govt_privacy) as avg_dnc_2018_govt_privacy
,avg(dnc_2018_min_wage) as avg_dnc_2018_min_wage
,avg(dnc_2018_pro_choice) as avg_dnc_2018_pro_choice
,avg(dnc_2018_citizenship) as avg_dnc_2018_citizenship
,avg(dnc_2018_college_funding) as avg_dnc_2018_college_funding
,avg(dnc_2018_gun_control) as avg_dnc_2018_gun_control
,avg(dnc_2018_paid_leave) as avg_dnc_2018_paid_leave
,avg(dnc_2018_progressive_tax) as avg_dnc_2018_progressive_tax
,avg(dnc_2018_volprop_overall_rank) as avg_dnc_2018_volprop_overall_rank
,avg(dnc_2018_volprop_phone_rank) as avg_dnc_2018_volprop_phone_rank
,avg(dnc_2018_volprop_walk_rank) as avg_dnc_2018_volprop_walk_rank
,avg(civis_2018_turnout) as avg_civis_2018_turnout
,avg(civis_2018_partisanship) as avg_civis_2018_partisanship
,avg(civis_2018_spanish_language_preference) as avg_civis_2018_spanish_language_preference
,avg(civis_2018_gotv) as avg_civis_2018_gotv
,avg(civis_2018_likely_race) as avg_civis_2018_likely_race
,avg(civis_2018_likely_race_confidence) as avg_civis_2018_likely_race_confidence
,avg(civis_2018_ballot_dropoff) as avg_civis_2018_ballot_dropoff
,avg(civis_2018_congressional_gotv_raw) as avg_civis_2018_congressional_gotv_raw
,avg(civis_2018_congressional_support) as avg_civis_2018_congressional_support
,avg(civis_2018_avev) as avg_civis_2018_avev
,avg(civis_2018_cultural_persuasion) as avg_civis_2018_cultural_persuasion
,avg(civis_2018_economic_persuasion) as avg_civis_2018_economic_persuasion
,avg(civis_2018_political_persuasion) as avg_civis_2018_political_persuasion
,avg(dnc_2018_high_school_only) as avg_dnc_2018_high_school_only
,avg(dnc_2018_income_rank) as avg_dnc_2018_income_rank
,avg(dnc_2018_catholic_tier) as avg_dnc_2018_catholic_tier
,avg(dnc_2018_evangelical_tier) as avg_dnc_2018_evangelical_tier
,avg(dnc_2018_otherchristian_tier) as avg_dnc_2018_otherchristian_tier
,avg(dnc_2018_nonchristian_tier) as avg_dnc_2018_nonchristian_tier
,avg(dnc_2018_party_support_score) as avg_dnc_2018_party_support_score
,avg(civis_2018_gotv_raw) as avg_civis_2018_gotv_raw
,avg(civis_2018_one_pct_persuasion) as avg_civis_2018_one_pct_persuasion
,avg(civis_2018_infrastructure_persuasion) as avg_civis_2018_infrastructure_persuasion
,avg(civis_2018_bipartisan_persuasion) as avg_civis_2018_bipartisan_persuasion
,avg(civis_2018_aca_persuasion) as avg_civis_2018_aca_persuasion
,avg(civis_2018_growth_persuasion) as avg_civis_2018_growth_persuasion
,avg(civis_2018_sexual_assault_persuasion) as avg_civis_2018_sexual_assault_persuasion
,avg(civis_2018_wall_persuasion) as avg_civis_2018_wall_persuasion
,avg(civis_2018_marijuana_persuasion) as avg_civis_2018_marijuana_persuasion
,avg(civis_2018_race_persuasion) as avg_civis_2018_race_persuasion
,avg(civis_2018_welcome_persuasion) as avg_civis_2018_welcome_persuasion
,avg(civis_2018_trump_persuasion) as avg_civis_2018_trump_persuasion
,avg(civis_2018_skills_persuasion) as avg_civis_2018_skills_persuasion
,avg(civis_2018_gop_persuasion) as avg_civis_2018_gop_persuasion
,avg(civis_2018_climate_persuasion) as avg_civis_2018_climate_persuasion
,avg(civis_2018_lgbt_persuasion) as avg_civis_2018_lgbt_persuasion
,avg(civis_2018_college_persuasion) as avg_civis_2018_college_persuasion
,avg(civis_2018_guns_persuasion) as avg_civis_2018_guns_persuasion
,avg(civis_2018_dreamers_persuasion) as avg_civis_2018_dreamers_persuasion
,avg(civis_2018_military_persuasion) as avg_civis_2018_military_persuasion
,avg(civis_2018_progressive_persuasion) as avg_civis_2018_progressive_persuasion
,avg(civis_2018_choice_persuasion) as avg_civis_2018_choice_persuasion
,avg(civis_2018_medicare_persuasion) as avg_civis_2018_medicare_persuasion
,avg(civis_2018_ballot_completion) as avg_civis_2018_ballot_completion
,avg(pres_2016_support) as avg_pres_2016_support
,avg(dnc_2016_party_support_score) as avg_dnc_2016_party_support_score
,avg(dnc_2016_turnout) as avg_dnc_2016_turnout
,avg(dnc_2016_ideology_liberal) as avg_dnc_2016_ideology_liberal
,avg(dnc_2016_ideology_moderate) as avg_dnc_2016_ideology_moderate
,avg(dnc_2016_ideology_conservative) as avg_dnc_2016_ideology_conservative
,avg(dnc_2016_high_school_only) as avg_dnc_2016_high_school_only
,avg(dnc_2016_income_rank) as avg_dnc_2016_income_rank
,avg(tsmart_2016_donor_likelihood) as avg_tsmart_2016_donor_likelihood
from 
phoenix_analytics.person
left join
phoenix_scores.all_scores_2020 using(person_id)
left join 
phoenix_scores.all_scores_2018 using(person_id)
left join 
phoenix_scores.all_scores_2016 using(person_id)
group by census_block_group_2010
)


set query_group to 'importers';

drop table if exists bernie_data_commons.phoenix_modeling_frame; 
create table bernie_data_commons.phoenix_modeling_frame 
distkey(person_id)
sortkey(person_id)
as (
  
select p.person_id
  ,x.jsonid
  

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
left join phoenix_scores.all_scores_2018 score using(person_id) 
left join phoenix_scores.all_scores_2016 score using(person_id) 
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
