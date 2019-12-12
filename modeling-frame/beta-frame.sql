--Data Dictionary https://docs.google.com/spreadsheets/d/183LBmZGNbWdjmGxuBY8GrbtXuoUMobcm5BO331vs5Og/edit?usp=sharing

set query_group to 'importers';

drop table if exists bernie_data_commons.phoenix_modeling_frame; 
create table bernie_data_commons.phoenix_modeling_frame 
distkey(person_id)
sortkey(person_id)
as (
  
select p.person_id
  ,x.jsonid
 
--Primary returns
  ,coalesce(pri.primary16_clinton,.5) as primary16_clinton
  ,coalesce(pri.primary16_sanders,.5) as primary16_sanders
  
--All scores
  -- Civis marriage, children, language, partisan/ideology scores
  ,coalesce(civis_2020_marriage,0.566609551) as civis_2020_marriage
  ,coalesce(dnc_2018_marriage,0.528516131) as dnc_2018_marriage
  ,coalesce(civis_2020_children_present,0.291702784) as civis_2020_children_present
  ,coalesce(civis_2020_partisanship,0.5836077) as civis_2020_partisanship
  ,coalesce(civis_2020_ideology_liberal,0.412724049) as civis_2020_ideology_liberal
  ,coalesce(civis_2020_spanish_language_preference,0.025251928) as civis_2020_spanish_language_preference
  ,coalesce(civis_2018_spanish_language_preference,0.02299987) as civis_2018_spanish_language_preference

  -- Civis 2018 support, turnout, gotv
  ,coalesce(civis_2018_turnout,0.464239577) as civis_2018_turnout
  ,coalesce(civis_2018_partisanship,0.552657781) as civis_2018_partisanship
  ,coalesce(civis_2018_gotv,0.444764104) as civis_2018_gotv
  ,coalesce(civis_2018_ballot_dropoff,0.536298285) as civis_2018_ballot_dropoff
  ,coalesce(civis_2018_congressional_gotv_raw,0.002071406) as civis_2018_congressional_gotv_raw
  ,coalesce(civis_2018_congressional_support,0.547379039) as civis_2018_congressional_support
  ,coalesce(civis_2018_avev,0.422649328) as civis_2018_avev

  -- DNC education and income
  ,coalesce(dnc_2018_college_graduate,0.434772353) as dnc_2018_college_graduate
  ,coalesce(dnc_2018_income_dollars,56053.269194657) as dnc_2018_income_dollars
  ,coalesce(dnc_2018_high_school_only,0.234392294) as dnc_2018_high_school_only
  ,coalesce(dnc_2018_income_rank,50) as dnc_2018_income_rank

  -- Civis race
  ,coalesce(civis_2020_race_native,0.020016087) as civis_2020_race_native
  ,coalesce(civis_2020_race_black,0.136500989) as civis_2020_race_black
  ,coalesce(civis_2020_race_latinx,0.103369184) as civis_2020_race_latinx
  ,coalesce(civis_2020_race_asian,0.044318742) as civis_2020_race_asian
  ,coalesce(civis_2020_race_white,0.695794994) as civis_2020_race_white

  -- Civis subethnicity
  ,coalesce(civis_2020_subeth_african_american,0.111452007) as civis_2020_subeth_african_american
  ,coalesce(civis_2020_subeth_west_indian,0.006705198) as civis_2020_subeth_west_indian
  ,coalesce(civis_2020_subeth_haitian,0.003665945) as civis_2020_subeth_haitian
  ,coalesce(civis_2020_subeth_african,0.008107618) as civis_2020_subeth_african
  ,coalesce(civis_2020_subeth_other_black,0.006570217) as civis_2020_subeth_other_black
  ,coalesce(civis_2020_subeth_mexican,0.052468483) as civis_2020_subeth_mexican
  ,coalesce(civis_2020_subeth_cuban,0.00489653) as civis_2020_subeth_cuban
  ,coalesce(civis_2020_subeth_puerto_rican,0.017962263) as civis_2020_subeth_puerto_rican
  ,coalesce(civis_2020_subeth_dominican,0.003085198) as civis_2020_subeth_dominican
  ,coalesce(civis_2020_subeth_other_latin_american,0.019031732) as civis_2020_subeth_other_latin_american
  ,coalesce(civis_2020_subeth_other_hispanic,0.005924973) as civis_2020_subeth_other_hispanic
  ,coalesce(civis_2020_subeth_chinese,0.010457775) as civis_2020_subeth_chinese
  ,coalesce(civis_2020_subeth_indian,0.010060997) as civis_2020_subeth_indian
  ,coalesce(civis_2020_subeth_filipino,0.007511224) as civis_2020_subeth_filipino
  ,coalesce(civis_2020_subeth_japanese,0.003227617) as civis_2020_subeth_japanese
  ,coalesce(civis_2020_subeth_vietnamese,0.003967237) as civis_2020_subeth_vietnamese
  ,coalesce(civis_2020_subeth_korean,0.003409507) as civis_2020_subeth_korean
  ,coalesce(civis_2020_subeth_other_asian,0.004695813) as civis_2020_subeth_other_asian
  ,coalesce(civis_2020_subeth_hmong,0.000988565) as civis_2020_subeth_hmong

  -- Civis religion
  ,coalesce(civis_2020_cultural_religion_jewish,0.039996867) as civis_2020_cultural_religion_jewish
  ,coalesce(civis_2020_cultural_religion_mormon,0.019002679) as civis_2020_cultural_religion_mormon
  ,coalesce(civis_2020_cultural_religion_muslim,0.013412685) as civis_2020_cultural_religion_muslim
  ,coalesce(civis_2020_cultural_religion_catholic,0.268376253) as civis_2020_cultural_religion_catholic
  ,coalesce(civis_2020_cultural_religion_evangelical,0.347880455) as civis_2020_cultural_religion_evangelical
  ,coalesce(civis_2020_cultural_religion_mainline_protestant,0.259510438) as civis_2020_cultural_religion_mainline_protestant
  ,coalesce(civis_2020_cultural_religion_hindu,0.010488034) as civis_2020_cultural_religion_hindu
  ,coalesce(civis_2020_cultural_religion_buddhist,0.019066914) as civis_2020_cultural_religion_buddhist

  -- Civis persuasion
  ,coalesce(civis_2018_gotv_raw,0.003834102) as civis_2018_gotv_raw
  ,coalesce(civis_2018_cultural_persuasion,-0.005461805) as civis_2018_cultural_persuasion
  ,coalesce(civis_2018_economic_persuasion,0.006528537) as civis_2018_economic_persuasion
  ,coalesce(civis_2018_political_persuasion,0.003801198) as civis_2018_political_persuasion
  ,coalesce(civis_2018_one_pct_persuasion,0.00611318) as civis_2018_one_pct_persuasion
  ,coalesce(civis_2018_infrastructure_persuasion,0.006191701) as civis_2018_infrastructure_persuasion
  ,coalesce(civis_2018_bipartisan_persuasion,0.006445597) as civis_2018_bipartisan_persuasion
  ,coalesce(civis_2018_aca_persuasion,0.005342091) as civis_2018_aca_persuasion
  ,coalesce(civis_2018_growth_persuasion,0.005146488) as civis_2018_growth_persuasion
  ,coalesce(civis_2018_sexual_assault_persuasion,0.009672641) as civis_2018_sexual_assault_persuasion
  ,coalesce(civis_2018_wall_persuasion,-0.001512978) as civis_2018_wall_persuasion
  ,coalesce(civis_2018_marijuana_persuasion,-0.00030366) as civis_2018_marijuana_persuasion
  ,coalesce(civis_2018_race_persuasion,0.002819312) as civis_2018_race_persuasion
  ,coalesce(civis_2018_welcome_persuasion,0.003711122) as civis_2018_welcome_persuasion
  ,coalesce(civis_2018_trump_persuasion,-0.000755541) as civis_2018_trump_persuasion
  ,coalesce(civis_2018_skills_persuasion,0.004798559) as civis_2018_skills_persuasion
  ,coalesce(civis_2018_gop_persuasion,0.003132949) as civis_2018_gop_persuasion
  ,coalesce(civis_2018_climate_persuasion,0.003127991) as civis_2018_climate_persuasion
  ,coalesce(civis_2018_lgbt_persuasion,0.004011263) as civis_2018_lgbt_persuasion
  ,coalesce(civis_2018_college_persuasion,0.005387874) as civis_2018_college_persuasion
  ,coalesce(civis_2018_guns_persuasion,0.00400441) as civis_2018_guns_persuasion
  ,coalesce(civis_2018_dreamers_persuasion,-0.004672808) as civis_2018_dreamers_persuasion
  ,coalesce(civis_2018_military_persuasion,0.00572671) as civis_2018_military_persuasion
  ,coalesce(civis_2018_progressive_persuasion,0.004168864) as civis_2018_progressive_persuasion
  ,coalesce(civis_2018_choice_persuasion,0.004149563) as civis_2018_choice_persuasion
  ,coalesce(civis_2018_medicare_persuasion,0.011460919) as civis_2018_medicare_persuasion
  ,coalesce(civis_2018_ballot_completion,0.463701714) as civis_2018_ballot_completion

  -- DNC party and ideology
  ,coalesce(dnc_2018_party_support_score,0.543546793) as dnc_2018_party_support_score
  ,coalesce(dnc_2018_downballot_defection_rank,50) as dnc_2018_downballot_defection_rank
  ,coalesce(dnc_2018_downballot_vote,0.524497071) as dnc_2018_downballot_vote
  ,coalesce(dnc_2018_ideology_liberal,0.265048665) as dnc_2018_ideology_liberal
  ,coalesce(dnc_2018_ideology_moderate,0.359360419) as dnc_2018_ideology_moderate
  ,coalesce(dnc_2018_ideology_conservative,0.375590915) as dnc_2018_ideology_conservative
  ,coalesce(dnc_2018_campaign_finance,0.536041155) as dnc_2018_campaign_finance

  -- DNC issues
  ,coalesce(dnc_2018_climate_change,0.615028224) as dnc_2018_climate_change
  ,coalesce(dnc_2018_govt_privacy,0.533446421) as dnc_2018_govt_privacy
  ,coalesce(dnc_2018_min_wage,0.591191554) as dnc_2018_min_wage
  ,coalesce(dnc_2018_pro_choice,0.541718063) as dnc_2018_pro_choice
  ,coalesce(dnc_2018_citizenship,0.554849089) as dnc_2018_citizenship
  ,coalesce(dnc_2018_college_funding,0.617279678) as dnc_2018_college_funding
  ,coalesce(dnc_2018_gun_control,0.562465658) as dnc_2018_gun_control
  ,coalesce(dnc_2018_paid_leave,0.657602321) as dnc_2018_paid_leave
  ,coalesce(dnc_2018_progressive_tax,0.600933257) as dnc_2018_progressive_tax
  ,coalesce(dnc_2018_volprop_overall_rank,50) as dnc_2018_volprop_overall_rank
  ,coalesce(dnc_2018_volprop_phone_rank,50) as dnc_2018_volprop_phone_rank
  ,coalesce(dnc_2018_volprop_walk_rank,51) as dnc_2018_volprop_walk_rank

  -- DNC 2016 
  ,coalesce(pres_2016_support,0.529941755) as pres_2016_support
  ,coalesce(dnc_2016_party_support_score,0.540886883) as dnc_2016_party_support_score
  ,coalesce(dnc_2016_turnout,0.740073165) as dnc_2016_turnout
  ,coalesce(dnc_2016_ideology_liberal,0.259230233) as dnc_2016_ideology_liberal
  ,coalesce(dnc_2016_ideology_moderate,0.367292204) as dnc_2016_ideology_moderate
  ,coalesce(dnc_2016_ideology_conservative,0.371972141) as dnc_2016_ideology_conservative
  ,coalesce(dnc_2016_high_school_only,0.232407721) as dnc_2016_high_school_only
  ,coalesce(dnc_2016_income_rank,55) as dnc_2016_income_rank
  ,coalesce(tsmart_2016_donor_likelihood,0.325387388) as tsmart_2016_donor_likelihood
  
-- Experian occupation codes  
  ,case when xp_occupation = 'K01' then 1 else 0 end as xp_occupation_k01
  ,case when xp_occupation = 'K02' then 1 else 0 end as xp_occupation_k02
  ,case when xp_occupation = 'K03' then 1 else 0 end as xp_occupation_k03
  ,case when xp_occupation = 'K04' then 1 else 0 end as xp_occupation_k04
  ,case when xp_occupation = 'K05' then 1 else 0 end as xp_occupation_k05
  ,case when xp_occupation = 'K06' then 1 else 0 end as xp_occupation_k06
  
-- Block Group ACS Features
  -- Industry and economic composition
  ,coalesce(xc_24070_e_10,10.5021808346473) as pct_prof_sci_mng
  ,coalesce(xc_24070_e_11,23.3169387120635) as pct_educ_health
  ,coalesce(xc_24070_e_12,9.76120640305069) as pct_arts_ent_hotel_food
  ,coalesce(xc_24070_e_13,4.9372763541136) as pct_other_serv
  ,coalesce(xc_24070_e_14,4.70981094468377) as pct_pub_admin
  ,coalesce(xc_24070_e_15,67.6719088419246) as pct_private
  ,coalesce(xc_24070_e_2,2.22482878526911) as pct_agriculture
  ,coalesce(xc_24070_e_3,6.29887799760338) as pct_construction
  ,coalesce(xc_24070_e_4,10.5900279493579) as pct_manufacturing
  ,coalesce(xc_24070_e_6,11.5310261382819) as pct_retail
  ,coalesce(xc_24070_e_7,5.05414222332237) as pct_transport_warehouse
  ,coalesce(xc_24070_e_5,2.58820902288239) as pct_wholesale
  ,coalesce(xc_24070_e_8,1.9808691752496) as pct_information
  ,coalesce(xc_24070_e_9,6.15837855795037) as pct_finance_insur
  ,coalesce(xc_24070_e_29,3.35201406323088) as pct_selfemployed_incorporated_biz
  ,coalesce(xc_24070_e_43,8.18953615208745) as pct_not_for_profit
  ,coalesce(xc_24070_e_57,14.1523126127464) as pct_government_worker
  ,coalesce(xc_24070_e_71,6.28800827128944) as pct_selfemployed_nonincorporated_biz

  -- Birth in last year
  ,coalesce(xb_13008_e_2,5.28599158612465) as pct_had_birth_in_last_year

  -- Language spoken at home
  ,coalesce(xc_16001_e_12,0.666356436745457) as pct_lang_at_home_russian_slavic
  ,coalesce(xc_16001_e_15,1.65284907167414) as pct_lang_at_home_indo_euro
  ,coalesce(xc_16001_e_18,0.322285583986244) as pct_lang_at_home_korean
  ,coalesce(xc_16001_e_2,80.2671499610826) as pct_lang_at_home_only_english
  ,coalesce(xc_16001_e_21,0.96546189141353) as pct_lang_at_home_chinese
  ,coalesce(xc_16001_e_24,0.407945143013321) as pct_lang_at_home_vietnamese
  ,coalesce(xc_16001_e_27,0.47831653476469) as pct_lang_at_home_tagalog
  ,coalesce(xc_16001_e_3,12.0587109495928) as pct_lang_at_home_spanish
  ,coalesce(xc_16001_e_30,0.828565265109512) as pct_lang_at_home_other_asian
  ,coalesce(xc_16001_e_33,0.344957984728002) as pct_lang_at_home_arabic
  ,coalesce(xc_16001_e_36,0.641450896064653) as pct_lang_at_home_other_lang
  ,coalesce(xc_16001_e_6,0.65794864225899) as pct_lang_at_home_french
  ,coalesce(xc_16001_e_9,0.483782984689971) as pct_lang_at_home_germanic

  -- Change of address
  ,coalesce(xb_07001_e_81,0.594762297133181) as pct_moved_from_abroad
  ,coalesce(xb_07001_e_49,3.09860403448412) as pct_moved_from_different_county_within_same_state
  ,coalesce(xb_07001_e_65,2.21059028553814) as pct_moved_from_different_state
  ,coalesce(xb_07001_e_33,8.71861633001596) as pct_moved_within_same_county
  ,coalesce(xb_07001_e_17,85.1532493413606) as pct_same_house_1_year_ago

  -- Foreign born
  ,coalesce(xb_05007_e_15,27.5581932832236) as pct_foreign_born_asia
  ,coalesce(xb_05007_e_2,19.0890989707642) as pct_foreign_born_europe
  ,coalesce(xb_05007_e_28,41.1432239245551) as pct_foreign_born_latin_america
  ,coalesce(xb_05007_e_82,10.1881488482489) as pct_foreign_born_other_areas
  ,coalesce(xb_05007_e_29,14.8401338549315) as pct_foreign_born_caribbean
  ,coalesce(xb_05007_e_42,54.2330559167257) as pct_foreign_born_central_america
  ,coalesce(xb_05007_e_43,51.6578188460353) as pct_foreign_born_mexico
  ,coalesce(xb_05007_e_56,22.4681717297962) as pct_foreign_born_other_central_america
  ,coalesce(xb_05007_e_69,17.2102764107977) as pct_foreign_born_south_america

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
    when civis_2020_likely_race = 'W' then 1
    when p.ethnicity_combined = 'W' 
    or ((ethnicity_combined is null or ethnicity_combined ='O')
        and(civis_2020_race_white > civis_2020_race_native
        and civis_2020_race_white > civis_2020_race_black
        and civis_2020_race_white > civis_2020_race_asian
        and civis_2020_race_white > civis_2020_race_latinx)) then 1 else 0 end as eth_white
  
  ,case 
    when civis_2020_likely_race = 'A' then 1
    when p.ethnicity_combined = 'A' 
    or ((ethnicity_combined is null or ethnicity_combined ='O')
        and(civis_2020_race_asian > civis_2020_race_native
        and civis_2020_race_asian > civis_2020_race_black
        and civis_2020_race_asian > civis_2020_race_white
        and civis_2020_race_asian > civis_2020_race_latinx)) then 1 else 0 end as eth_asian

  ,case 
    when civis_2020_likely_race = 'B' then 1
    when p.ethnicity_combined = 'B' 
    or ((ethnicity_combined is null or ethnicity_combined ='O')
        and(civis_2020_race_black > civis_2020_race_native
        and civis_2020_race_black > civis_2020_race_latinx
        and civis_2020_race_black > civis_2020_race_asian
        and civis_2020_race_black > civis_2020_race_white)) then 1 else 0 end as eth_afam
  
  ,case 
    when civis_2020_likely_race = 'H' then 1
    when p.ethnicity_combined = 'H' 
    or ((ethnicity_combined is null or ethnicity_combined = 'O')
        and(civis_2020_race_latinx > civis_2020_race_native
        and civis_2020_race_latinx  > civis_2020_race_black
        and civis_2020_race_latinx  > civis_2020_race_asian
        and civis_2020_race_latinx  > civis_2020_race_white)) then 1 else 0 end as eth_hispanic

  ,case 
    when civis_2020_likely_race = 'N' then 1
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
--left join l2.demographics l2 using(lalvoterid)
left join phoenix_census.acs_current on block_group_id = p.census_block_group_2010
left join bernie_nmarchio2.primaryreturns16 pri on p.county_fips = right(census_county_fips,'3') and p.state_fips = left(lpad(census_county_fips,5,'000'),2)

  where p.is_deceased = false -- is alive
  and p.reg_record_merged = false -- removes some duplicates
  and p.reg_on_current_file = true --  voter was on the last voter file that the DNC processed and not eligible to vote in primaries
  and p.reg_voter_flag = true -- voters who are registered to vote (i.e. have a registration status of active or inactive) even if they have moved states and their new state has not updated their file to reflect this yet

);

grant select on table bernie_data_commons.phoenix_modeling_frame to group bernie_data;
