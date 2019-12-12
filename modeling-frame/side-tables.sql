DROP TABLE IF EXISTS bernie_nmarchio2.all_scores_impute;
CREATE TABLE bernie_nmarchio2.all_scores_impute
  DISTSTYLE KEY
  DISTKEY (census_block_group_2010)
  SORTKEY (census_block_group_2010)
  AS (select census_block_group_2010
,avg(civis_2020_race_native) as avg_civis_2020_race_native
,avg(civis_2020_race_black) as avg_civis_2020_race_black
,avg(civis_2020_race_latinx) as avg_civis_2020_race_latinx
,avg(civis_2020_race_asian) as avg_civis_2020_race_asian
,avg(civis_2020_race_white) as avg_civis_2020_race_white
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
);

DROP TABLE IF EXISTS bernie_nmarchio2.tsmart_impute ;
CREATE TABLE bernie_nmarchio2.tsmart_impute 
  DISTSTYLE KEY
  DISTKEY (census_block_group_2010)
  SORTKEY (census_block_group_2010)
  AS (select
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
);
