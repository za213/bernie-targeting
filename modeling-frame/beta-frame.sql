


create temp table sanders_clinton_impute
distkey(person_id)
sortkey(person_id)
as (
select 
person_id
,coalesce(demo_p2016_clinton_bucket,  avg_impute_clinton, avg_impute_clinton_state) as demo_p2016_clinton_bucket
,coalesce(demo_p2016_sanders_bucket, avg_impute_sanders, avg_impute_sanders_state) as demo_p2016_sanders_bucket
from

(select 
person_id
,demo_p2016_clinton_bucket
,demo_p2016_sanders_bucket
,avg(l2.demo_p2016_clinton_bucket) over (partition by p.state_code || p.county_fips) as avg_impute_clinton
,avg(l2.demo_p2016_clinton_bucket) over (partition by p.state_code) as avg_impute_clinton_state
,avg(l2.demo_p2016_sanders_bucket) over (partition by p.state_code || p.county_fips) as avg_impute_sanders
,avg(l2.demo_p2016_sanders_bucket) over (partition by p.state_code) as avg_impute_sanders_state

from
phoenix_analytics.person p 
left join bernie_data_commons.master_xwalk_dnc x using(person_id)
left join 
(select  * 
 ,round(((replace(electionreturns_p16_cnty_pct_clinton_d, '%', '')::float)/10)::float,4) as demo_p2016_clinton_bucket
,round(((replace(electionreturns_p16_cnty_pct_sanders_d, '%', '')::float)/10)::float,4) as demo_p2016_sanders_bucket from l2.demographics) l2 using(lalvoterid)
)
);



drop table if exists bernie_data_commons.phoenix_modeling_frame; 
create table bernie_data_commons.phoenix_modeling_frame 
distkey(person_id)
sortkey(person_id)
as (
  
select p.person_id
  ,x.lalvoterid
  ,x.jsonid
  ,p.state_code
  ,p.county_name
  ,p.county_fips
  ,p.dnc_precinct_id
  ,p.us_cong_district_latest
  ,p.media_market
  ,imp1.demo_p2016_clinton_bucket
  ,imp1.demo_p2016_sanders_bucket

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
  ,case 
    when coalesce(p.age_combined,l2.voters_age::int)  < 23
        then 1 else 0 end as age_18_22
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int)  < 30
        then 1 else 0 end as age_23_29
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int)  < 40
        then 1 else 0 end as age_30_39
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int)  < 50
        then 1 else 0 end as age_40_49
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int)  < 60
        then 1 else 0 end as age_50_59
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int)  < 70
        then 1 else 0 end as age_60_69
  ,case  
    when coalesce(p.age_combined,l2.voters_age::int)  < 80
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

left join sanders_clinton_impute imp1 using(person_id)


  where p.is_deceased = false -- is alive
  and p.reg_record_merged = false -- removes some duplicates
  and p.reg_on_current_file = true --  voter was on the last voter file that the DNC processed and not eligible to vote in primaries
  and p.reg_voter_flag = true -- voters who are registered to vote (i.e. have a registration status of active or inactive) even if they have moved states and their new state has not updated their file to reflect this yet


  );

grant select on table bernie_data_commons.phoenix_modeling_frame to group bernie_data;
