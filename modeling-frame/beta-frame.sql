
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

creat temp table tsmart_impute as (
select 
census_block_group_2010
,avg(ts_tsmart_partisan_score) as avg_ts_tsmart_partisan_score
,avg(ts_tsmart_presidential_general_turnout_score) as avg_ts_tsmart_presidential_general_turnout_score
,avg(ts_tsmart_midterm_general_turnout_score) as avg_ts_tsmart_midterm_general_turnout_score
,avg(ts_tsmart_offyear_general_turnout_score) as avg_ts_tsmart_offyear_general_turnout_score
,avg(ts_tsmart_presidential_primary_turnout_score) as avg_ts_tsmart_presidential_primary_turnout_score
,avg(ts_tsmart_non_presidential_primary_turnout_score) as avg_ts_tsmart_non_presidential_primary_turnout_score
,avg(ts_tsmart_midterm_general_enthusiasm_score) as avg_ts_tsmart_midterm_general_enthusiasm_score
,avg(ts_tsmart_p_white) as avg_ts_tsmart_p_white
,avg(ts_tsmart_p_afam) as avg_ts_tsmart_p_afam
,avg(ts_tsmart_p_hisp) as avg_ts_tsmart_p_hisp
,avg(ts_tsmart_p_natam) as avg_ts_tsmart_p_natam
,avg(ts_tsmart_p_asian) as avg_ts_tsmart_p_asian
,avg(ts_tsmart_local_voter_score) as avg_ts_tsmart_local_voter_score
,avg(ts_tsmart_tea_party_score) as avg_ts_tsmart_tea_party_score
,avg(ts_tsmart_ideology_score) as avg_ts_tsmart_ideology_score
,avg(ts_tsmart_moral_authority_score) as avg_ts_tsmart_moral_authority_score
,avg(ts_tsmart_moral_care_score) as avg_ts_tsmart_moral_care_score
,avg(ts_tsmart_moral_equality_score) as avg_ts_tsmart_moral_equality_score
,avg(ts_tsmart_moral_equity_score) as avg_ts_tsmart_moral_equity_score
,avg(ts_tsmart_moral_loyalty_score) as avg_ts_tsmart_moral_loyalty_score
,avg(ts_tsmart_moral_purity_score) as avg_ts_tsmart_moral_purity_score
,avg(ts_tsmart_college_graduate_score) as avg_ts_tsmart_college_graduate_score
,avg(ts_tsmart_high_school_only_score) as avg_ts_tsmart_high_school_only_score
,avg(ts_tsmart_prochoice_score) as avg_ts_tsmart_prochoice_score
,avg(ts_tsmart_path_to_citizen_score) as avg_ts_tsmart_path_to_citizen_score
,avg(ts_tsmart_climate_change_score) as avg_ts_tsmart_climate_change_score
,avg(ts_tsmart_gun_control_score) as avg_ts_tsmart_gun_control_score
,avg(ts_tsmart_gunowner_score) as avg_ts_tsmart_gunowner_score
,avg(ts_tsmart_trump_resistance_score) as avg_ts_tsmart_trump_resistance_score
,avg(ts_tsmart_trump_support_score) as avg_ts_tsmart_trump_support_score
,avg(ts_tsmart_veteran_score) as avg_ts_tsmart_veteran_score
,avg(ts_tsmart_activist_score) as avg_ts_tsmart_activist_score
,avg(ts_tsmart_working_class_score) as avg_ts_tsmart_working_class_score
,avg(predictwise_authoritarianism_score) as avg_predictwise_authoritarianism_score
,avg(predictwise_compassion_score) as avg_predictwise_compassion_score
,avg(predictwise_economic_populism_score) as avg_predictwise_economic_populism_score
,avg(predictwise_free_trade_score) as avg_predictwise_free_trade_score
,avg(predictwise_globalism_score) as avg_predictwise_globalism_score
,avg(predictwise_guns_score) as avg_predictwise_guns_score
,avg(predictwise_healthcare_women_score) as avg_predictwise_healthcare_women_score
,avg(predictwise_healthcare_score) as avg_predictwise_healthcare_score
,avg(predictwise_immigrants_score) as avg_predictwise_immigrants_score
,avg(predictwise_military_score) as avg_predictwise_military_score
,avg(predictwise_populism_score) as avg_predictwise_populism_score
,avg(predictwise_poor_score) as avg_predictwise_poor_score
,avg(predictwise_racial_resentment_score) as avg_predictwise_racial_resentment_score
,avg(predictwise_regulation_score) as avg_predictwise_regulation_score
,avg(predictwise_religious_freedom_score) as avg_predictwise_religious_freedom_score
,avg(predictwise_taxes_score) as avg_predictwise_taxes_score
,avg(predictwise_traditionalism_score) as avg_predictwise_traditionalism_score
,avg(predictwise_trust_in_institutions_score) as avg_predictwise_trust_in_institutions_score
,avg(predictwise_environmentalism_score) as avg_predictwise_environmentalism_score
,avg(predictwise_presidential_score) as avg_predictwise_presidential_score

,avg(enh_addr_size) as avg_enh_addr_size
,avg(enh_tsmart_enhanced_hh_size) as avg_enh_tsmart_enhanced_hh_size
,avg(enh_tsmart_enhanced_hh_number_males) as avg_enh_tsmart_enhanced_hh_number_males
,avg(enh_tsmart_enhanced_hh_number_females) as avg_enh_tsmart_enhanced_hh_number_females
,avg(enh_tsmart_enhanced_hh_number_registered) as avg_enh_tsmart_enhanced_hh_number_registered
,avg(enh_tsmart_enhanced_hh_number_unregistered) as avg_enh_tsmart_enhanced_hh_number_unregistered
,avg(enh_tsmart_enhanced_hh_number_dems) as avg_enh_tsmart_enhanced_hh_number_dems
,avg(enh_tsmart_enhanced_hh_number_reps) as avg_enh_tsmart_enhanced_hh_number_reps
,avg(enh_tsmart_enhanced_hh_number_others) as avg_enh_tsmart_enhanced_hh_number_others

,avg(fec_zip_avg_contribution_amount) as avg_fec_zip_avg_contribution_amount
,avg(fec_zip_contribution_category_corporation) as avg_fec_zip_contribution_category_corporation
,avg(fec_zip_contribution_category_democrat) as avg_fec_zip_contribution_category_democrat
,avg(fec_zip_contribution_category_house) as avg_fec_zip_contribution_category_house
,avg(fec_zip_contribution_category_labor_organization) as avg_fec_zip_contribution_category_labor_organization
,avg(fec_zip_contribution_category_membership_organization) as avg_fec_zip_contribution_category_membership_organization
,avg(fec_zip_contribution_category_presidential) as avg_fec_zip_contribution_category_presidential
,avg(fec_zip_contribution_category_qualified_party) as avg_fec_zip_contribution_category_qualified_party
,avg(fec_zip_contribution_category_republican) as avg_fec_zip_contribution_category_republican
,avg(fec_zip_contribution_category_senate) as avg_fec_zip_contribution_category_senate
,avg(fec_zip_contribution_category_trade_association) as avg_fec_zip_contribution_category_trade_association
,avg(fec_zip_contribution_category_unaffiliated) as avg_fec_zip_contribution_category_unaffiliated
,avg(fec_zip_total_contribution_amount) as avg_fec_zip_total_contribution_amount
,avg(fec_zip_total_contributions) as avg_fec_zip_total_contributions
,avg(gsyn_pct_catholic) as avg_gsyn_pct_catholic
,avg(gsyn_pct_jewish) as avg_gsyn_pct_jewish
,avg(gsyn_synth_county_pct_of_dem_fec_contributions) as avg_gsyn_synth_county_pct_of_dem_fec_contributions
,avg(gsyn_synth_county_sum_fec_contribution_count_democrat) as avg_gsyn_synth_county_sum_fec_contribution_count_democrat
,avg(gsyn_synth_county_sum_fec_contribution_count_republican) as avg_gsyn_synth_county_sum_fec_contribution_count_republican
,avg(gsyn_synth_county_sum_individual_in_county) as avg_gsyn_synth_county_sum_individual_in_county
,avg(gsyn_synth_county_total_fec_contributions) as avg_gsyn_synth_county_total_fec_contributions
,avg(gsyn_synth_hh_average_age) as avg_gsyn_synth_hh_average_age
,avg(gsyn_synth_hh_distinct_last_names_in_hh) as avg_gsyn_synth_hh_distinct_last_names_in_hh
,avg(gsyn_synth_hh_distinct_races_in_hh) as avg_gsyn_synth_hh_distinct_races_in_hh
,avg(gsyn_synth_hh_pct_registered) as avg_gsyn_synth_hh_pct_registered
,avg(gsyn_synth_hh_sum_dem_primary_votes_cast_count) as avg_gsyn_synth_hh_sum_dem_primary_votes_cast_count
,avg(gsyn_synth_hh_sum_individuals_in_hh) as avg_gsyn_synth_hh_sum_individuals_in_hh
,avg(gsyn_synth_hh_sum_primary_votes_cast_count) as avg_gsyn_synth_hh_sum_primary_votes_cast_count
,avg(gsyn_synth_hh_sum_rep_primary_votes_cast_count) as avg_gsyn_synth_hh_sum_rep_primary_votes_cast_count
,avg(gsyn_synth_hh_sum_total_votes_cast_count) as avg_gsyn_synth_hh_sum_total_votes_cast_count
,avg(gsyn_synth_hh_total_primary_votes_person) as avg_gsyn_synth_hh_total_primary_votes_person
,avg(gsyn_synth_hh_total_votes_per_person) as avg_gsyn_synth_hh_total_votes_per_person
,avg(gsyn_synth_zip5_pct_catholic) as avg_gsyn_synth_zip5_pct_catholic
,avg(gsyn_synth_zip5_pct_dem_primary_votes) as avg_gsyn_synth_zip5_pct_dem_primary_votes
,avg(gsyn_synth_zip5_pct_jewish) as avg_gsyn_synth_zip5_pct_jewish
,avg(gsyn_synth_zip5_pct_of_dem_fec_contributions) as avg_gsyn_synth_zip5_pct_of_dem_fec_contributions
,avg(gsyn_synth_zip5_pct_of_dems_per_reg_count) as avg_gsyn_synth_zip5_pct_of_dems_per_reg_count
,avg(gsyn_synth_zip5_sum_dem_primary_votes_cast_count) as avg_gsyn_synth_zip5_sum_dem_primary_votes_cast_count
,avg(gsyn_synth_zip5_sum_fec_contribution_count_democrat) as avg_gsyn_synth_zip5_sum_fec_contribution_count_democrat
,avg(gsyn_synth_zip5_sum_fec_contribution_count_republican) as avg_gsyn_synth_zip5_sum_fec_contribution_count_republican
,avg(gsyn_synth_zip5_sum_individuals_in_zip5) as avg_gsyn_synth_zip5_sum_individuals_in_zip5
,avg(gsyn_synth_zip5_sum_primary_votes_cast_count) as avg_gsyn_synth_zip5_sum_primary_votes_cast_count
,avg(gsyn_synth_zip5_sum_registered) as avg_gsyn_synth_zip5_sum_registered
,avg(gsyn_synth_zip5_sum_unregistered) as avg_gsyn_synth_zip5_sum_unregistered
,avg(gsyn_synth_zip5_sum_zip5_democrat) as avg_gsyn_synth_zip5_sum_zip5_democrat
,avg(gsyn_synth_zip5_sum_zip5_republican) as avg_gsyn_synth_zip5_sum_zip5_republican
,avg(gsyn_synth_zip5_total_fec_contributions) as avg_gsyn_synth_zip5_total_fec_contributions
,avg(gsyn_synth_zip9_pct_dem_primary_votes) as avg_gsyn_synth_zip9_pct_dem_primary_votes
,avg(gsyn_synth_zip9_pct_female) as avg_gsyn_synth_zip9_pct_female
,avg(gsyn_synth_zip9_pct_male) as avg_gsyn_synth_zip9_pct_male

from 
phoenix_analytics.person
left join
phoenix_consumer.tsmart_consumer using(person_id)
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
 
--Primary returns
  ,pri.primary16_clinton
  ,pri.primary16_sanders
  
--All scores

  -- Civis marriage, children, language, partisan/ideology scores
  ,coalesce(civis_2020_marriage,avg_civis_2020_marriage,0.5) as civis_2020_marriage
  ,coalesce(civis_2020_children_present,avg_civis_2020_children_present,0.5) as civis_2020_children_present
  ,coalesce(civis_2020_partisanship,avg_civis_2020_partisanship,0.5) as civis_2020_partisanship
  ,coalesce(civis_2020_ideology_liberal,avg_civis_2020_ideology_liberal,0.5) as civis_2020_ideology_liberal
  ,coalesce(civis_2020_spanish_language_preference,avg_civis_2020_spanish_language_preference,0.5) as civis_2020_spanish_language_preference
  ,coalesce(civis_2018_spanish_language_preference,avg_civis_2018_spanish_language_preference,0.5) as civis_2018_spanish_language_preference
  
  -- Civis 2018 support, turnout, gotv
  ,coalesce(civis_2018_turnout,avg_civis_2018_turnout,0.5) as civis_2018_turnout
  ,coalesce(civis_2018_partisanship,avg_civis_2018_partisanship,0.5) as civis_2018_partisanship
  ,coalesce(civis_2018_gotv,avg_civis_2018_gotv,0.5) as civis_2018_gotv
  ,coalesce(civis_2018_likely_race,avg_civis_2018_likely_race,0.5) as civis_2018_likely_race
  ,coalesce(civis_2018_likely_race_confidence,avg_civis_2018_likely_race_confidence,0.5) as civis_2018_likely_race_confidence
  ,coalesce(civis_2018_ballot_dropoff,avg_civis_2018_ballot_dropoff,0.5) as civis_2018_ballot_dropoff
  ,coalesce(civis_2018_congressional_gotv_raw,avg_civis_2018_congressional_gotv_raw,0.5) as civis_2018_congressional_gotv_raw
  ,coalesce(civis_2018_congressional_support,avg_civis_2018_congressional_support,0.5) as civis_2018_congressional_support
  ,coalesce(civis_2018_avev,avg_civis_2018_avev,0.5) as civis_2018_avev
  
  -- DNC education and income
  ,coalesce(dnc_2018_college_graduate,avg_dnc_2018_college_graduate,0.5) as dnc_2018_college_graduate
  ,coalesce(dnc_2018_income_dollars,avg_dnc_2018_income_dollars,0.5) as dnc_2018_income_dollars
  ,coalesce(dnc_2018_high_school_only,avg_dnc_2018_high_school_only,0.5) as dnc_2018_high_school_only
  ,coalesce(dnc_2018_income_rank,avg_dnc_2018_income_rank,0.5) as dnc_2018_income_rank
  
  -- Civis race
  ,coalesce(civis_2020_race_native,avg_civis_2020_race_native,0.5) as civis_2020_race_native
  ,coalesce(civis_2020_race_black,avg_civis_2020_race_black,0.5) as civis_2020_race_black
  ,coalesce(civis_2020_race_latinx,avg_civis_2020_race_latinx,0.5) as civis_2020_race_latinx
  ,coalesce(civis_2020_race_asian,avg_civis_2020_race_asian,0.5) as civis_2020_race_asian
  ,coalesce(civis_2020_race_white,avg_civis_2020_race_white,0.5) as civis_2020_race_white
  ,coalesce(civis_2020_likely_race,avg_civis_2020_likely_race,0.5) as civis_2020_likely_race
  ,coalesce(civis_2020_likely_race_confidence,avg_civis_2020_likely_race_confidence,0.5) as civis_2020_likely_race_confidence
  
  -- Civis subethnicity
  ,coalesce(civis_2020_subeth_african_american,avg_civis_2020_subeth_african_american,0.5) as civis_2020_subeth_african_american
  ,coalesce(civis_2020_subeth_west_indian,avg_civis_2020_subeth_west_indian,0.5) as civis_2020_subeth_west_indian
  ,coalesce(civis_2020_subeth_haitian,avg_civis_2020_subeth_haitian,0.5) as civis_2020_subeth_haitian
  ,coalesce(civis_2020_subeth_african,avg_civis_2020_subeth_african,0.5) as civis_2020_subeth_african
  ,coalesce(civis_2020_subeth_other_black,avg_civis_2020_subeth_other_black,0.5) as civis_2020_subeth_other_black
  ,coalesce(civis_2020_subeth_mexican,avg_civis_2020_subeth_mexican,0.5) as civis_2020_subeth_mexican
  ,coalesce(civis_2020_subeth_cuban,avg_civis_2020_subeth_cuban,0.5) as civis_2020_subeth_cuban
  ,coalesce(civis_2020_subeth_puerto_rican,avg_civis_2020_subeth_puerto_rican,0.5) as civis_2020_subeth_puerto_rican
  ,coalesce(civis_2020_subeth_dominican,avg_civis_2020_subeth_dominican,0.5) as civis_2020_subeth_dominican
  ,coalesce(civis_2020_subeth_other_latin_american,avg_civis_2020_subeth_other_latin_american,0.5) as civis_2020_subeth_other_latin_american
  ,coalesce(civis_2020_subeth_other_hispanic,avg_civis_2020_subeth_other_hispanic,0.5) as civis_2020_subeth_other_hispanic
  ,coalesce(civis_2020_subeth_chinese,avg_civis_2020_subeth_chinese,0.5) as civis_2020_subeth_chinese
  ,coalesce(civis_2020_subeth_indian,avg_civis_2020_subeth_indian,0.5) as civis_2020_subeth_indian
  ,coalesce(civis_2020_subeth_filipino,avg_civis_2020_subeth_filipino,0.5) as civis_2020_subeth_filipino
  ,coalesce(civis_2020_subeth_japanese,avg_civis_2020_subeth_japanese,0.5) as civis_2020_subeth_japanese
  ,coalesce(civis_2020_subeth_vietnamese,avg_civis_2020_subeth_vietnamese,0.5) as civis_2020_subeth_vietnamese
  ,coalesce(civis_2020_subeth_korean,avg_civis_2020_subeth_korean,0.5) as civis_2020_subeth_korean
  ,coalesce(civis_2020_subeth_other_asian,avg_civis_2020_subeth_other_asian,0.5) as civis_2020_subeth_other_asian
  ,coalesce(civis_2020_subeth_hmong,avg_civis_2020_subeth_hmong,0.5) as civis_2020_subeth_hmong

  -- Civis religion
  ,coalesce(civis_2020_cultural_religion_jewish,avg_civis_2020_cultural_religion_jewish,0.5) as civis_2020_cultural_religion_jewish
  ,coalesce(civis_2020_cultural_religion_mormon,avg_civis_2020_cultural_religion_mormon,0.5) as civis_2020_cultural_religion_mormon
  ,coalesce(civis_2020_cultural_religion_muslim,avg_civis_2020_cultural_religion_muslim,0.5) as civis_2020_cultural_religion_muslim
  ,coalesce(civis_2020_cultural_religion_catholic,avg_civis_2020_cultural_religion_catholic,0.5) as civis_2020_cultural_religion_catholic
  ,coalesce(civis_2020_cultural_religion_evangelical,avg_civis_2020_cultural_religion_evangelical,0.5) as civis_2020_cultural_religion_evangelical
  ,coalesce(civis_2020_cultural_religion_mainline_protestant,avg_civis_2020_cultural_religion_mainline_protestant,0.5) as civis_2020_cultural_religion_mainline_protestant
  ,coalesce(civis_2020_cultural_religion_hindu,avg_civis_2020_cultural_religion_hindu,0.5) as civis_2020_cultural_religion_hindu
  ,coalesce(civis_2020_cultural_religion_buddhist,avg_civis_2020_cultural_religion_buddhist,0.5) as civis_2020_cultural_religion_buddhist
  
  -- Civis persuasion
  ,coalesce(civis_2018_cultural_persuasion,avg_civis_2018_cultural_persuasion,0.5) as civis_2018_cultural_persuasion
  ,coalesce(civis_2018_economic_persuasion,avg_civis_2018_economic_persuasion,0.5) as civis_2018_economic_persuasion
  ,coalesce(civis_2018_political_persuasion,avg_civis_2018_political_persuasion,0.5) as civis_2018_political_persuasion
  ,coalesce(civis_2018_gotv_raw,avg_civis_2018_gotv_raw,0.5) as civis_2018_gotv_raw
  ,coalesce(civis_2018_one_pct_persuasion,avg_civis_2018_one_pct_persuasion,0.5) as civis_2018_one_pct_persuasion
  ,coalesce(civis_2018_infrastructure_persuasion,avg_civis_2018_infrastructure_persuasion,0.5) as civis_2018_infrastructure_persuasion
  ,coalesce(civis_2018_bipartisan_persuasion,avg_civis_2018_bipartisan_persuasion,0.5) as civis_2018_bipartisan_persuasion
  ,coalesce(civis_2018_aca_persuasion,avg_civis_2018_aca_persuasion,0.5) as civis_2018_aca_persuasion
  ,coalesce(civis_2018_growth_persuasion,avg_civis_2018_growth_persuasion,0.5) as civis_2018_growth_persuasion
  ,coalesce(civis_2018_sexual_assault_persuasion,avg_civis_2018_sexual_assault_persuasion,0.5) as civis_2018_sexual_assault_persuasion
  ,coalesce(civis_2018_wall_persuasion,avg_civis_2018_wall_persuasion,0.5) as civis_2018_wall_persuasion
  ,coalesce(civis_2018_marijuana_persuasion,avg_civis_2018_marijuana_persuasion,0.5) as civis_2018_marijuana_persuasion
  ,coalesce(civis_2018_race_persuasion,avg_civis_2018_race_persuasion,0.5) as civis_2018_race_persuasion
  ,coalesce(civis_2018_welcome_persuasion,avg_civis_2018_welcome_persuasion,0.5) as civis_2018_welcome_persuasion
  ,coalesce(civis_2018_trump_persuasion,avg_civis_2018_trump_persuasion,0.5) as civis_2018_trump_persuasion
  ,coalesce(civis_2018_skills_persuasion,avg_civis_2018_skills_persuasion,0.5) as civis_2018_skills_persuasion
  ,coalesce(civis_2018_gop_persuasion,avg_civis_2018_gop_persuasion,0.5) as civis_2018_gop_persuasion
  ,coalesce(civis_2018_climate_persuasion,avg_civis_2018_climate_persuasion,0.5) as civis_2018_climate_persuasion
  ,coalesce(civis_2018_lgbt_persuasion,avg_civis_2018_lgbt_persuasion,0.5) as civis_2018_lgbt_persuasion
  ,coalesce(civis_2018_college_persuasion,avg_civis_2018_college_persuasion,0.5) as civis_2018_college_persuasion
  ,coalesce(civis_2018_guns_persuasion,avg_civis_2018_guns_persuasion,0.5) as civis_2018_guns_persuasion
  ,coalesce(civis_2018_dreamers_persuasion,avg_civis_2018_dreamers_persuasion,0.5) as civis_2018_dreamers_persuasion
  ,coalesce(civis_2018_military_persuasion,avg_civis_2018_military_persuasion,0.5) as civis_2018_military_persuasion
  ,coalesce(civis_2018_progressive_persuasion,avg_civis_2018_progressive_persuasion,0.5) as civis_2018_progressive_persuasion
  ,coalesce(civis_2018_choice_persuasion,avg_civis_2018_choice_persuasion,0.5) as civis_2018_choice_persuasion
  ,coalesce(civis_2018_medicare_persuasion,avg_civis_2018_medicare_persuasion,0.5) as civis_2018_medicare_persuasion
  ,coalesce(civis_2018_ballot_completion,avg_civis_2018_ballot_completion,0.5) as civis_2018_ballot_completion
  
  -- DNC party and ideology
  ,coalesce(dnc_2018_party_support_score,avg_dnc_2018_party_support_score,0.5) as dnc_2018_party_support_score
  ,coalesce(dnc_2018_downballot_defection_rank,avg_dnc_2018_downballot_defection_rank,0.5) as dnc_2018_downballot_defection_rank
  ,coalesce(dnc_2018_downballot_vote,avg_dnc_2018_downballot_vote,0.5) as dnc_2018_downballot_vote
  ,coalesce(dnc_2018_ideology_liberal,avg_dnc_2018_ideology_liberal,0.5) as dnc_2018_ideology_liberal
  ,coalesce(dnc_2018_ideology_moderate,avg_dnc_2018_ideology_moderate,0.5) as dnc_2018_ideology_moderate
  ,coalesce(dnc_2018_ideology_conservative,avg_dnc_2018_ideology_conservative,0.5) as dnc_2018_ideology_conservative
  ,coalesce(dnc_2018_campaign_finance,avg_dnc_2018_campaign_finance,0.5) as dnc_2018_campaign_finance
  
  -- DNC issues
  ,coalesce(dnc_2018_climate_change,avg_dnc_2018_climate_change,0.5) as dnc_2018_climate_change
  ,coalesce(dnc_2018_govt_privacy,avg_dnc_2018_govt_privacy,0.5) as dnc_2018_govt_privacy
  ,coalesce(dnc_2018_min_wage,avg_dnc_2018_min_wage,0.5) as dnc_2018_min_wage
  ,coalesce(dnc_2018_pro_choice,avg_dnc_2018_pro_choice,0.5) as dnc_2018_pro_choice
  ,coalesce(dnc_2018_citizenship,avg_dnc_2018_citizenship,0.5) as dnc_2018_citizenship
  ,coalesce(dnc_2018_college_funding,avg_dnc_2018_college_funding,0.5) as dnc_2018_college_funding
  ,coalesce(dnc_2018_gun_control,avg_dnc_2018_gun_control,0.5) as dnc_2018_gun_control
  ,coalesce(dnc_2018_paid_leave,avg_dnc_2018_paid_leave,0.5) as dnc_2018_paid_leave
  ,coalesce(dnc_2018_progressive_tax,avg_dnc_2018_progressive_tax,0.5) as dnc_2018_progressive_tax
  ,coalesce(dnc_2018_volprop_overall_rank,avg_dnc_2018_volprop_overall_rank,0.5) as dnc_2018_volprop_overall_rank
  ,coalesce(dnc_2018_volprop_phone_rank,avg_dnc_2018_volprop_phone_rank,0.5) as dnc_2018_volprop_phone_rank
  ,coalesce(dnc_2018_volprop_walk_rank,avg_dnc_2018_volprop_walk_rank,0.5) as dnc_2018_volprop_walk_rank
  
  -- DNC 2016 
  ,coalesce(pres_2016_support,avg_pres_2016_support,0.5) as pres_2016_support
  ,coalesce(dnc_2016_party_support_score,avg_dnc_2016_party_support_score,0.5) as dnc_2016_party_support_score
  ,coalesce(dnc_2016_turnout,avg_dnc_2016_turnout,0.5) as dnc_2016_turnout
  ,coalesce(dnc_2016_ideology_liberal,avg_dnc_2016_ideology_liberal,0.5) as dnc_2016_ideology_liberal
  ,coalesce(dnc_2016_ideology_moderate,avg_dnc_2016_ideology_moderate,0.5) as dnc_2016_ideology_moderate
  ,coalesce(dnc_2016_ideology_conservative,avg_dnc_2016_ideology_conservative,0.5) as dnc_2016_ideology_conservative
  ,coalesce(dnc_2016_high_school_only,avg_dnc_2016_high_school_only,0.5) as dnc_2016_high_school_only
  ,coalesce(dnc_2016_income_rank,avg_dnc_2016_income_rank,0.5) as dnc_2016_income_rank
  ,coalesce(tsmart_2016_donor_likelihood,avg_tsmart_2016_donor_likelihood,0.5) as tsmart_2016_donor_likelihood
  
-- Experian occupation codes  
  ,case when xp_occupation = 'K01' then 1 else 0 end as xp_occupation_k01
  ,case when xp_occupation = 'K02' then 1 else 0 end as xp_occupation_k02
  ,case when xp_occupation = 'K03' then 1 else 0 end as xp_occupation_k03
  ,case when xp_occupation = 'K04' then 1 else 0 end as xp_occupation_k04
  ,case when xp_occupation = 'K05' then 1 else 0 end as xp_occupation_k05
  ,case when xp_occupation = 'K06' then 1 else 0 end as xp_occupation_k06
  
-- ACS Features (industry, women who gave birth, language spoken at home, migration, citizenship, naturalization)
  -- Industry
  ,xc_24070_e_10
  ,xc_24070_e_11
  ,xc_24070_e_12
  ,xc_24070_e_13
  ,xc_24070_e_14
  ,xc_24070_e_15
  ,xc_24070_e_2
  ,xc_24070_e_3
  ,xc_24070_e_4
  ,xc_24070_e_6
  ,xc_24070_e_7
  ,xc_24070_e_5
  ,xc_24070_e_8
  ,xc_24070_e_9
  ,xc_24070_e_29
  ,xc_24070_e_43
  ,xc_24070_e_57
  ,xc_24070_e_71
  -- Nativity
  ,xb_13008_e_2
  ,xb_13008_e_3
  ,xb_13008_e_6
  ,xb_13008_e_4
  ,xb_13008_e_5
  ,xb_13008_e_7
  ,xb_13008_e_8
  -- Language spoken at home
  ,xc_16001_e_12
  ,xc_16001_e_15
  ,xc_16001_e_18
  ,xc_16001_e_2
  ,xc_16001_e_21
  ,xc_16001_e_24
  ,xc_16001_e_27
  ,xc_16001_e_3
  ,xc_16001_e_30
  ,xc_16001_e_33
  ,xc_16001_e_36
  ,xc_16001_e_6
  ,xc_16001_e_9
  ,xc_16001_e_14
  ,xc_16001_e_17
  ,xc_16001_e_20
  ,xc_16001_e_23
  ,xc_16001_e_26
  ,xc_16001_e_29
  ,xc_16001_e_32
  ,xc_16001_e_35
  ,xc_16001_e_38
  ,xc_16001_e_5
  ,xc_16001_e_8
  -- Geo mobility
  ,xb_07001_e_81
  ,xb_07001_e_49
  ,xb_07001_e_65
  ,xb_07001_e_33
  ,xb_07001_e_17
  -- Citizenship
  ,xb_05007_e_15
  ,xb_05007_e_2
  ,xb_05007_e_28
  ,xb_05007_e_82
  ,xb_05007_e_29
  ,xb_05007_e_42
  ,xb_05007_e_43
  ,xb_05007_e_56
  ,xb_05007_e_69
  -- Immigration origin and year
  ,xb_05007_e_37
  ,xb_05007_e_23
  ,xb_05007_e_51
  ,xb_05007_e_64
  ,xb_05007_e_77
  ,xb_05007_e_34
  ,xb_05007_e_20
  ,xb_05007_e_7
  ,xb_05007_e_48
  ,xb_05007_e_61
  ,xb_05007_e_74
  ,xb_05007_e_31
  ,xb_05007_e_17
  ,xb_05007_e_4
  ,xb_05007_e_45
  ,xb_05007_e_58
  ,xb_05007_e_71
  ,xb_05007_e_40
  ,xb_05007_e_26
  ,xb_05007_e_13
  ,xb_05007_e_54
  ,xb_05007_e_67
  ,xb_05007_e_80

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
left join all_scores_impute using(census_block_group_2010)
left join phoenix_consumer.tsmart_consumer tc using(person_id) 
left join bernie_data_commons.master_xwalk_dnc x using(person_id)
left join l2.demographics l2 using(lalvoterid)
left join phoenix_census.acs_current on block_group_id = p.census_block_group_2010
left join bernie_nmarchio2.primaryreturns16 pri on p.county_fips = right(census_county_fips,'3') and p.state_fips = left(lpad(census_county_fips,5,'000'),2)

  where p.is_deceased = false -- is alive
  and p.reg_record_merged = false -- removes some duplicates
  and p.reg_on_current_file = true --  voter was on the last voter file that the DNC processed and not eligible to vote in primaries
  and p.reg_voter_flag = true -- voters who are registered to vote (i.e. have a registration status of active or inactive) even if they have moved states and their new state has not updated their file to reflect this yet

);

grant select on table bernie_data_commons.phoenix_modeling_frame to group bernie_data;
