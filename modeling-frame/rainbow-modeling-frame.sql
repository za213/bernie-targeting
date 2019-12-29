
-- Data dictionary: https://docs.google.com/spreadsheets/d/1O1a4SdNBuPFMRT97__IeD1624OFDFafCSGQAuclDrFU/edit?usp=sharing
-- Feature list and dictionary import: https://platform.civisanalytics.com/spa/#/imports/53801807

set query_group to 'importers';
set wlm_query_slot_count to 3;

--truncate bernie_data_commons.rainbow_modeling_frame;
drop table if exists bernie_data_commons.rainbow_modeling_frame; 
create table bernie_data_commons.rainbow_modeling_frame
--insert into bernie_data_commons.rainbow_modeling_frame
distkey(person_id)
sortkey(person_id)
as 
(select p.person_id
  ,p.state_code
  ,p.census_block_group_2010
  
--Individual scores
  -- Marital status, children in household, spanish language preference, partisan/ideology scores
  ,coalesce(as20.civis_2020_marriage,0.566609551) as civis_2020_marriage
  --,coalesce(as18.dnc_2018_marriage,0.528516131) as dnc_2018_marriage
  ,coalesce(as20.civis_2020_children_present,0.291702784) as civis_2020_children_present
  ,coalesce(as20.civis_2020_partisanship,0.5836077) as civis_2020_partisanship
  ,coalesce(as20.civis_2020_ideology_liberal,0.412724049) as civis_2020_ideology_liberal
  ,coalesce(as20.civis_2020_spanish_language_preference,0.025251928) as civis_2020_spanish_language_preference
  --,coalesce(as18.civis_2018_spanish_language_preference,0.02299987) as civis_2018_spanish_language_preference

  -- 2018 support, turnout, gotv
  ,coalesce(as18.civis_2018_turnout,0.464239577) as civis_2018_turnout
  ,coalesce(as18.civis_2018_partisanship,0.552657781) as civis_2018_partisanship
  ,coalesce(as18.civis_2018_gotv,0.444764104) as civis_2018_gotv
  ,coalesce(as18.civis_2018_ballot_dropoff,0.536298285) as civis_2018_ballot_dropoff
  ,coalesce(as18.civis_2018_congressional_gotv_raw,0.002071406) as civis_2018_congressional_gotv_raw
  ,coalesce(as18.civis_2018_congressional_support,0.547379039) as civis_2018_congressional_support
  ,coalesce(as18.civis_2018_avev,0.422649328) as civis_2018_avev

  -- Education and income
  ,coalesce(as18.dnc_2018_college_graduate,0.434772353) as dnc_2018_college_graduate
  ,coalesce(as18.dnc_2018_income_dollars,56053.269194657) as dnc_2018_income_dollars
  ,coalesce(as18.dnc_2018_high_school_only,0.234392294) as dnc_2018_high_school_only
  ,coalesce(as18.dnc_2018_income_rank,50) as dnc_2018_income_rank

  -- Race and ethnicity
  ,coalesce(as20.civis_2020_race_native,0.020016087) as civis_2020_race_native
  ,coalesce(as20.civis_2020_race_black,0.136500989) as civis_2020_race_black
  ,coalesce(as20.civis_2020_race_latinx,0.103369184) as civis_2020_race_latinx
  ,coalesce(as20.civis_2020_race_asian,0.044318742) as civis_2020_race_asian
  ,coalesce(as20.civis_2020_race_white,0.695794994) as civis_2020_race_white

  -- Subethnicity
  ,coalesce(as20.civis_2020_subeth_african_american,0.111452007) as civis_2020_subeth_african_american
  ,coalesce(as20.civis_2020_subeth_west_indian,0.006705198) as civis_2020_subeth_west_indian
  ,coalesce(as20.civis_2020_subeth_haitian,0.003665945) as civis_2020_subeth_haitian
  ,coalesce(as20.civis_2020_subeth_african,0.008107618) as civis_2020_subeth_african
  ,coalesce(as20.civis_2020_subeth_other_black,0.006570217) as civis_2020_subeth_other_black
  ,coalesce(as20.civis_2020_subeth_mexican,0.052468483) as civis_2020_subeth_mexican
  ,coalesce(as20.civis_2020_subeth_cuban,0.00489653) as civis_2020_subeth_cuban
  ,coalesce(as20.civis_2020_subeth_puerto_rican,0.017962263) as civis_2020_subeth_puerto_rican
  ,coalesce(as20.civis_2020_subeth_dominican,0.003085198) as civis_2020_subeth_dominican
  ,coalesce(as20.civis_2020_subeth_other_latin_american,0.019031732) as civis_2020_subeth_other_latin_american
  ,coalesce(as20.civis_2020_subeth_other_hispanic,0.005924973) as civis_2020_subeth_other_hispanic
  ,coalesce(as20.civis_2020_subeth_chinese,0.010457775) as civis_2020_subeth_chinese
  ,coalesce(as20.civis_2020_subeth_indian,0.010060997) as civis_2020_subeth_indian
  ,coalesce(as20.civis_2020_subeth_filipino,0.007511224) as civis_2020_subeth_filipino
  ,coalesce(as20.civis_2020_subeth_japanese,0.003227617) as civis_2020_subeth_japanese
  ,coalesce(as20.civis_2020_subeth_vietnamese,0.003967237) as civis_2020_subeth_vietnamese
  ,coalesce(as20.civis_2020_subeth_korean,0.003409507) as civis_2020_subeth_korean
  ,coalesce(as20.civis_2020_subeth_other_asian,0.004695813) as civis_2020_subeth_other_asian
  ,coalesce(as20.civis_2020_subeth_hmong,0.000988565) as civis_2020_subeth_hmong

  -- Religion
  ,coalesce(as20.civis_2020_cultural_religion_jewish,0.039996867) as civis_2020_cultural_religion_jewish
  ,coalesce(as20.civis_2020_cultural_religion_mormon,0.019002679) as civis_2020_cultural_religion_mormon
  ,coalesce(as20.civis_2020_cultural_religion_muslim,0.013412685) as civis_2020_cultural_religion_muslim
  ,coalesce(as20.civis_2020_cultural_religion_catholic,0.268376253) as civis_2020_cultural_religion_catholic
  ,coalesce(as20.civis_2020_cultural_religion_evangelical,0.347880455) as civis_2020_cultural_religion_evangelical
  ,coalesce(as20.civis_2020_cultural_religion_mainline_protestant,0.259510438) as civis_2020_cultural_religion_mainline_protestant
  ,coalesce(as20.civis_2020_cultural_religion_hindu,0.010488034) as civis_2020_cultural_religion_hindu
  ,coalesce(as20.civis_2020_cultural_religion_buddhist,0.019066914) as civis_2020_cultural_religion_buddhist
  ,case when l2.religions_description = 'Islamic' then 1 else 0 end as religion_muslim 

  -- Message persuasion in terms of average treatment effects (ATEs)
  ,coalesce(as18.civis_2018_gotv_raw,0.003834102) as civis_2018_gotv_raw
  ,coalesce(as18.civis_2018_cultural_persuasion,-0.005461805) as civis_2018_cultural_persuasion
  ,coalesce(as18.civis_2018_economic_persuasion,0.006528537) as civis_2018_economic_persuasion
  ,coalesce(as18.civis_2018_political_persuasion,0.003801198) as civis_2018_political_persuasion
  -- Economic messages ATEs
  ,coalesce(as18.civis_2018_infrastructure_persuasion,0.006191701) as civis_2018_infrastructure_persuasion
  ,coalesce(as18.civis_2018_aca_persuasion,0.005342091) as civis_2018_aca_persuasion
  ,coalesce(as18.civis_2018_one_pct_persuasion,0.00611318) as civis_2018_one_pct_persuasion
  ,coalesce(as18.civis_2018_progressive_persuasion,0.004168864) as civis_2018_progressive_persuasion
  ,coalesce(as18.civis_2018_college_persuasion,0.005387874) as civis_2018_college_persuasion
  ,coalesce(as18.civis_2018_medicare_persuasion,0.011460919) as civis_2018_medicare_persuasion
  ,coalesce(as18.civis_2018_skills_persuasion,0.004798559) as civis_2018_skills_persuasion
  -- Cultural messages ATEs
  ,coalesce(as18.civis_2018_sexual_assault_persuasion,0.009672641) as civis_2018_sexual_assault_persuasion
  ,coalesce(as18.civis_2018_wall_persuasion,-0.001512978) as civis_2018_wall_persuasion
  ,coalesce(as18.civis_2018_marijuana_persuasion,-0.00030366) as civis_2018_marijuana_persuasion
  ,coalesce(as18.civis_2018_race_persuasion,0.002819312) as civis_2018_race_persuasion
  ,coalesce(as18.civis_2018_guns_persuasion,0.00400441) as civis_2018_guns_persuasion
  ,coalesce(as18.civis_2018_climate_persuasion,0.003127991) as civis_2018_climate_persuasion
  ,coalesce(as18.civis_2018_lgbt_persuasion,0.004011263) as civis_2018_lgbt_persuasion
  ,coalesce(as18.civis_2018_dreamers_persuasion,-0.004672808) as civis_2018_dreamers_persuasion
  ,coalesce(as18.civis_2018_military_persuasion,0.00572671) as civis_2018_military_persuasion
  ,coalesce(as18.civis_2018_choice_persuasion,0.004149563) as civis_2018_choice_persuasion
  ,coalesce(as18.civis_2018_ballot_completion,0.463701714) as civis_2018_ballot_completion
  -- Political messages ATEs
  ,coalesce(as18.civis_2018_growth_persuasion,0.005146488) as civis_2018_growth_persuasion
  ,coalesce(as18.civis_2018_bipartisan_persuasion,0.006445597) as civis_2018_bipartisan_persuasion
  ,coalesce(as18.civis_2018_gop_persuasion,0.003132949) as civis_2018_gop_persuasion
  ,coalesce(as18.civis_2018_welcome_persuasion,0.003711122) as civis_2018_welcome_persuasion
  ,coalesce(as18.civis_2018_trump_persuasion,-0.000755541) as civis_2018_trump_persuasion
 
  -- Most salient economic message
  ,case when greatest(as18.civis_2018_one_pct_persuasion,as18.civis_2018_progressive_persuasion) = as18.civis_2018_one_pct_persuasion then 1
  else 0 end as tax_the_1pct_voter
  ,case when greatest(as18.civis_2018_one_pct_persuasion,as18.civis_2018_progressive_persuasion) = as18.civis_2018_progressive_persuasion then 1
  else 0 end as generic_progressive_voter
  -- Most salient cultural message
  ,case when greatest(as18.civis_2018_choice_persuasion,as18.civis_2018_sexual_assault_persuasion,as18.civis_2018_race_persuasion,as18.civis_2018_climate_persuasion) = as18.civis_2018_sexual_assault_persuasion then 1
  else 0 end as metoo_voter
  ,case when greatest(as18.civis_2018_choice_persuasion,as18.civis_2018_sexual_assault_persuasion,as18.civis_2018_race_persuasion,as18.civis_2018_climate_persuasion) = as18.civis_2018_choice_persuasion then 1
  else 0 end as reproductive_rights_voter
  ,case when greatest(as18.civis_2018_choice_persuasion,as18.civis_2018_sexual_assault_persuasion,as18.civis_2018_race_persuasion,as18.civis_2018_climate_persuasion) = as18.civis_2018_race_persuasion then 1
  else 0 end as end_discrimination_voter
  ,case when greatest(as18.civis_2018_choice_persuasion,as18.civis_2018_sexual_assault_persuasion,as18.civis_2018_race_persuasion,as18.civis_2018_climate_persuasion) = as18.civis_2018_climate_persuasion then 1
  else 0 end as climate_change_voter
  -- Most salient political message
  ,case when greatest(as18.civis_2018_trump_persuasion,as18.civis_2018_gop_persuasion) = as18.civis_2018_trump_persuasion then 1
  else 0 end as anti_trump_voter
  ,case when greatest(as18.civis_2018_bipartisan_persuasion,as18.civis_2018_gop_persuasion) = as18.civis_2018_bipartisan_persuasion then 1
  else 0 end as restore_unity_voter

  -- DNC party and ideology
  ,coalesce(as18.dnc_2018_party_support_score,0.543546793) as dnc_2018_party_support_score
  ,coalesce(as18.dnc_2018_downballot_defection_rank,50) as dnc_2018_downballot_defection_rank
  ,coalesce(as18.dnc_2018_downballot_vote,0.524497071) as dnc_2018_downballot_vote
  ,coalesce(as18.dnc_2018_ideology_liberal,0.265048665) as dnc_2018_ideology_liberal
  ,coalesce(as18.dnc_2018_ideology_moderate,0.359360419) as dnc_2018_ideology_moderate
  ,coalesce(as18.dnc_2018_ideology_conservative,0.375590915) as dnc_2018_ideology_conservative
  ,coalesce(as18.dnc_2018_campaign_finance,0.536041155) as dnc_2018_campaign_finance

  -- DNC issues
  ,coalesce(as18.dnc_2018_climate_change,0.615028224) as dnc_2018_climate_change
  ,coalesce(as18.dnc_2018_govt_privacy,0.533446421) as dnc_2018_govt_privacy
  ,coalesce(as18.dnc_2018_min_wage,0.591191554) as dnc_2018_min_wage
  ,coalesce(as18.dnc_2018_pro_choice,0.541718063) as dnc_2018_pro_choice
  ,coalesce(as18.dnc_2018_citizenship,0.554849089) as dnc_2018_citizenship
  ,coalesce(as18.dnc_2018_college_funding,0.617279678) as dnc_2018_college_funding
  ,coalesce(as18.dnc_2018_gun_control,0.562465658) as dnc_2018_gun_control
  ,coalesce(as18.dnc_2018_paid_leave,0.657602321) as dnc_2018_paid_leave
  ,coalesce(as18.dnc_2018_progressive_tax,0.600933257) as dnc_2018_progressive_tax
  ,coalesce(as18.dnc_2018_volprop_overall_rank,50) as dnc_2018_volprop_overall_rank
  ,coalesce(as18.dnc_2018_volprop_phone_rank,50) as dnc_2018_volprop_phone_rank
  ,coalesce(as18.dnc_2018_volprop_walk_rank,51) as dnc_2018_volprop_walk_rank

  -- DNC 2016 scores
  ,coalesce(as16.pres_2016_support,0.529941755) as pres_2016_support
  ,coalesce(as16.dnc_2016_party_support_score,0.540886883) as dnc_2016_party_support_score
  ,coalesce(as16.dnc_2016_turnout,0.740073165) as dnc_2016_turnout
  ,coalesce(as16.dnc_2016_ideology_liberal,0.259230233) as dnc_2016_ideology_liberal
  ,coalesce(as16.dnc_2016_ideology_moderate,0.367292204) as dnc_2016_ideology_moderate
  ,coalesce(as16.dnc_2016_ideology_conservative,0.371972141) as dnc_2016_ideology_conservative
  ,coalesce(as16.dnc_2016_high_school_only,0.232407721) as dnc_2016_high_school_only
  ,coalesce(as16.dnc_2016_income_rank,55) as dnc_2016_income_rank
  ,coalesce(as16.tsmart_2016_donor_likelihood,0.325387388) as tsmart_2016_donor_likelihood
 
  -- TargetSmart turnout 
  ,coalesce(tc.ts_tsmart_partisan_score,53.615874715767) as ts_tsmart_partisan_score
  ,coalesce(tc.ts_tsmart_presidential_general_turnout_score,72.1139112743012) as ts_tsmart_presidential_general_turnout_score
  ,coalesce(tc.ts_tsmart_midterm_general_turnout_score,49.2156783912222) as ts_tsmart_midterm_general_turnout_score
  ,coalesce(tc.ts_tsmart_offyear_general_turnout_score,15.2742025259369) as ts_tsmart_offyear_general_turnout_score
  ,coalesce(tc.ts_tsmart_presidential_primary_turnout_score,37.3231147653052) as ts_tsmart_presidential_primary_turnout_score
  ,coalesce(tc.ts_tsmart_non_presidential_primary_turnout_score,17.8722169888302) as ts_tsmart_non_presidential_primary_turnout_score
  ,coalesce(tc.ts_tsmart_midterm_general_enthusiasm_score,40.6038213693376) as ts_tsmart_midterm_general_enthusiasm_score
  ,coalesce(tc.ts_tsmart_local_voter_score,35.6934520560535) as ts_tsmart_local_voter_score
  
  -- TargetSmart ideology and issue scores
  ,coalesce(tc.ts_tsmart_tea_party_score,46.0826663961952) as ts_tsmart_tea_party_score
  ,coalesce(tc.ts_tsmart_ideology_score,58.2238378150888) as ts_tsmart_ideology_score
  ,coalesce(tc.ts_tsmart_moral_authority_score,40.7086519676708) as ts_tsmart_moral_authority_score
  ,coalesce(tc.ts_tsmart_moral_care_score,48.4815620568045) as ts_tsmart_moral_care_score
  ,coalesce(tc.ts_tsmart_moral_equality_score,52.5460436289233) as ts_tsmart_moral_equality_score
  ,coalesce(tc.ts_tsmart_moral_equity_score,48.5739288914354) as ts_tsmart_moral_equity_score
  ,coalesce(tc.ts_tsmart_moral_loyalty_score,49.7838475335626) as ts_tsmart_moral_loyalty_score
  ,coalesce(tc.ts_tsmart_moral_purity_score,46.0161865880091) as ts_tsmart_moral_purity_score
  ,coalesce(tc.ts_tsmart_college_graduate_score,42.7505476530988) as ts_tsmart_college_graduate_score
  ,coalesce(tc.ts_tsmart_high_school_only_score,50.8139362209521) as ts_tsmart_high_school_only_score
  ,coalesce(tc.ts_tsmart_prochoice_score,56.2537590536877) as ts_tsmart_prochoice_score
  ,coalesce(tc.ts_tsmart_path_to_citizen_score,54.5802946861594) as ts_tsmart_path_to_citizen_score
  ,coalesce(tc.ts_tsmart_climate_change_score,57.5431390157507) as ts_tsmart_climate_change_score
  ,coalesce(tc.ts_tsmart_gun_control_score,54.7735550919433) as ts_tsmart_gun_control_score
  ,coalesce(tc.ts_tsmart_gunowner_score,54.0492868926705) as ts_tsmart_gunowner_score
  ,coalesce(tc.ts_tsmart_trump_resistance_score,39.8009590035495) as ts_tsmart_trump_resistance_score
  ,coalesce(tc.ts_tsmart_trump_support_score,48.8257967230227) as ts_tsmart_trump_support_score
  ,coalesce(tc.ts_tsmart_veteran_score,38.026382268614) as ts_tsmart_veteran_score
  ,coalesce(tc.ts_tsmart_activist_score,42.7382851129463) as ts_tsmart_activist_score
  ,coalesce(tc.ts_tsmart_working_class_score,58.6177878423767) as ts_tsmart_working_class_score
  
  -- PredictWise scores
  ,coalesce(tc.predictwise_authoritarianism_score,71) as predictwise_authoritarianism_score
  ,coalesce(tc.predictwise_compassion_score,55) as predictwise_compassion_score
  ,coalesce(tc.predictwise_economic_populism_score,43) as predictwise_economic_populism_score
  ,coalesce(tc.predictwise_free_trade_score,43) as predictwise_free_trade_score
  ,coalesce(tc.predictwise_globalism_score,46) as predictwise_globalism_score
  ,coalesce(tc.predictwise_guns_score,42) as predictwise_guns_score
  ,coalesce(tc.predictwise_healthcare_women_score,53) as predictwise_healthcare_women_score
  ,coalesce(tc.predictwise_healthcare_score,45) as predictwise_healthcare_score
  ,coalesce(tc.predictwise_immigrants_score,41) as predictwise_immigrants_score
  ,coalesce(tc.predictwise_military_score,53) as predictwise_military_score
  ,coalesce(tc.predictwise_populism_score,38) as predictwise_populism_score
  ,coalesce(tc.predictwise_poor_score,51) as predictwise_poor_score
  ,coalesce(tc.predictwise_racial_resentment_score,66) as predictwise_racial_resentment_score
  ,coalesce(tc.predictwise_regulation_score,59) as predictwise_regulation_score
  ,coalesce(tc.predictwise_religious_freedom_score,49) as predictwise_religious_freedom_score
  ,coalesce(tc.predictwise_taxes_score,44) as predictwise_taxes_score
  ,coalesce(tc.predictwise_traditionalism_score,52) as predictwise_traditionalism_score
  ,coalesce(tc.predictwise_trust_in_institutions_score,45) as predictwise_trust_in_institutions_score
  ,coalesce(tc.predictwise_environmentalism_score,53) as predictwise_environmentalism_score
  ,coalesce(tc.predictwise_presidential_score,55) as predictwise_presidential_score
  
   -- Synthetic registration 
  ,coalesce(tc.gsyn_synth_hh_pct_registered,95.974742397142) as gsyn_synth_hh_pct_registered
  ,coalesce(tc.gsyn_synth_hh_total_primary_votes_person,1.62222639559819) as gsyn_synth_hh_total_primary_votes_person
  ,coalesce(tc.gsyn_synth_zip5_pct_dem_primary_votes,0.233256642021729) as gsyn_synth_zip5_pct_dem_primary_votes
  ,coalesce(tc.gsyn_synth_zip5_pct_of_dem_fec_contributions,0.529875692892941) as gsyn_synth_zip5_pct_of_dem_fec_contributions
  ,coalesce(tc.gsyn_synth_zip5_pct_of_dems_per_reg_count,0.494239755293577) as gsyn_synth_zip5_pct_of_dems_per_reg_count
  ,coalesce(tc.gsyn_synth_zip5_sum_dem_primary_votes_cast_count,7515) as gsyn_synth_zip5_sum_dem_primary_votes_cast_count
  ,coalesce(tc.gsyn_synth_zip5_sum_fec_contribution_count_democrat,85) as gsyn_synth_zip5_sum_fec_contribution_count_democrat
  ,coalesce(tc.gsyn_synth_zip9_pct_dem_primary_votes,0.24470400461151) as gsyn_synth_zip9_pct_dem_primary_votes
  ,coalesce(tc.gsyn_synth_zip5_sum_registered,19430) as gsyn_synth_zip5_sum_registered
  ,coalesce(tc.gsyn_synth_zip5_sum_unregistered,5666) as gsyn_synth_zip5_sum_unregistered
  ,coalesce(tc.gsyn_synth_zip5_sum_zip5_democrat,10504) as gsyn_synth_zip5_sum_zip5_democrat
  ,coalesce(tc.gsyn_synth_zip5_sum_zip5_republican,7051) as gsyn_synth_zip5_sum_zip5_republican
 
  -- FEC contributions 
  ,coalesce(tc.fec_zip_contribution_category_democrat,514867) as fec_zip_contribution_category_democrat
  ,coalesce(tc.fec_zip_contribution_category_labor_organization,14300) as fec_zip_contribution_category_labor_organization
  ,coalesce(tc.fec_zip_contribution_category_presidential,184526) as fec_zip_contribution_category_presidential
  ,coalesce(tc.fec_zip_contribution_category_republican,400768) as fec_zip_contribution_category_republican
  ,coalesce(tc.fec_zip_contribution_category_senate,223010) as fec_zip_contribution_category_senate
  ,coalesce(tc.fec_zip_contribution_category_trade_association,49267) as fec_zip_contribution_category_trade_association
  /*
  ,coalesce(tc.enh_addr_size,40) as enh_addr_size
  ,coalesce(tc.fec_zip_avg_contribution_amount,285.292351174354) as fec_zip_avg_contribution_amount
  ,coalesce(tc.fec_zip_contribution_category_corporation,92676) as fec_zip_contribution_category_corporation
  ,coalesce(tc.fec_zip_contribution_category_house,289501) as fec_zip_contribution_category_house
  ,coalesce(tc.fec_zip_contribution_category_membership_organization,40105) as fec_zip_contribution_category_membership_organization
  ,coalesce(tc.fec_zip_contribution_category_qualified_party,225967) as fec_zip_contribution_category_qualified_party
  ,coalesce(tc.fec_zip_contribution_category_unaffiliated,487791) as fec_zip_contribution_category_unaffiliated
  ,coalesce(tc.fec_zip_total_contribution_amount,1408448) as fec_zip_total_contribution_amount
  ,coalesce(tc.fec_zip_total_contributions,3919) as fec_zip_total_contributions
  ,coalesce(tc.gsyn_synth_hh_average_age,50.3527862005709) as gsyn_synth_hh_average_age
  ,coalesce(tc.gsyn_synth_hh_total_votes_per_person,5.47473457667572) as gsyn_synth_hh_total_votes_per_person
  ,coalesce(tc.gsyn_synth_zip5_pct_catholic,14.4101302276016) as gsyn_synth_zip5_pct_catholic
  ,coalesce(tc.gsyn_synth_zip5_pct_jewish,1.10422757474685) as gsyn_synth_zip5_pct_jewish
  ,coalesce(tc.gsyn_synth_zip5_sum_fec_contribution_count_republican,51) as gsyn_synth_zip5_sum_fec_contribution_count_republican
  ,coalesce(tc.gsyn_synth_zip5_sum_individuals_in_zip5,25065) as gsyn_synth_zip5_sum_individuals_in_zip5
  ,coalesce(tc.gsyn_synth_zip5_sum_primary_votes_cast_count,33423) as gsyn_synth_zip5_sum_primary_votes_cast_count
  ,coalesce(tc.gsyn_synth_zip5_total_fec_contributions,141) as gsyn_synth_zip5_total_fec_contributions
  */
  
  -- Experian occupation groups
  ,case when tc.xp_occupation = 'K01' or tc.xpg_ind_lvl_occupation_group in ('I1','K1') then 1 else 0 end as xp_occupation_k01_manager
  ,case when tc.xp_occupation = 'K02' or tc.xpg_ind_lvl_occupation_group in ('I2','K2') then 1 else 0 end as xp_occupation_k02_technical
  ,case when tc.xp_occupation = 'K03' or tc.xpg_ind_lvl_occupation_group in ('I3','K3') then 1 else 0 end as xp_occupation_k03_professional
  ,case when tc.xp_occupation = 'K04' or tc.xpg_ind_lvl_occupation_group in ('I4','K4') then 1 else 0 end as xp_occupation_k04_sales
  ,case when tc.xp_occupation = 'K05' or tc.xpg_ind_lvl_occupation_group in ('I5','K5') then 1 else 0 end as xp_occupation_k05_office
  ,case when tc.xp_occupation = 'K06' or tc.xpg_ind_lvl_occupation_group in ('I6','K6') then 1 else 0 end as xp_occupation_k06_bluecollar
  ,case when tc.xpg_ind_lvl_occupation_group in ('I7','K7') or tc.xpg_ind_lvl_occupation_code = '15' then 1 else 0 end as xpg_occupation_farmer
  ,case when tc.xpg_ind_lvl_occupation_group in ('I9','K9') or tc.xpg_ind_lvl_occupation_code = '08' then 1 else 0 end as xpg_occupation_retiree
  -- Professional / Managerial Service occupations
  ,case when tc.xpg_ind_lvl_occupation_code in ('02','32') then 1 else 0 end as xpg_occupation_professional
  ,case when tc.xpg_ind_lvl_occupation_code in ('03','10') then 1 else 0 end as xpg_occupation_manager
  ,case when tc.xpg_ind_lvl_occupation_code in ('04','05','06') then 1 else 0 end as xpg_occupation_office
  ,case when tc.xpg_ind_lvl_occupation_code in ('37','53','18','38') then 1 else 0 end as xpg_occupation_law_fin_accnt
  ,case when tc.xpg_ind_lvl_occupation_code = '36' then 1 else 0 end as xpg_occupation_engineer
  ,case when tc.xpg_ind_lvl_occupation_code = '23' then 1 else 0 end as xpg_occupation_tech
  -- Healthcare / Allied Health occupations
  ,case when tc.xpg_ind_lvl_occupation_code in ('44','45','46','47','49') then 1 else 0 end as xpg_occupation_physicians
  ,case when tc.xpg_ind_lvl_occupation_code = '48' then 1 else 0 end as xpg_occupation_nurse
  ,case when tc.xpg_ind_lvl_occupation_code in ('39','40','41','42','43') then 1 else 0 end as xpg_occupation_therapy_socialwork
  -- Union occupations
  ,case when tc.xpg_ind_lvl_occupation_code = '21' then 1 else 0 end as xpg_occupation_teacher
  ,case when tc.xpg_ind_lvl_occupation_code in ('07','35') then 1 else 0 end as xpg_occupation_trades
  ,case when tc.xpg_memberships_union_member <= 20 then 1 else 0 end as xpg_union_member
  -- Local Service occupations
  ,case when tc.xpg_ind_lvl_occupation_code = '30' then 1 else 0 end as xpg_occupation_barbers_salon
  ,case when tc.xpg_ind_lvl_occupation_code = '12' then 1 else 0 end as xpg_occupation_driver
  ,case when tc.xpg_ind_lvl_occupation_code = '31' then 1 else 0 end as xpg_occupation_real_estate
  ,case when tc.xpg_ind_lvl_occupation_code in ('11','16') then 1 else 0 end as xpg_occupation_self
  -- Public Admin occupations
  ,case when tc.xpg_ind_lvl_occupation_code = '14' then 1 else 0 end as xpg_occupation_public_admin
  ,case when tc.xpg_ind_lvl_occupation_code = '13' then 1 else 0 end as xpg_occupation_military

-- County, Tract, and Block Group Geographic Data
  -- Geographic embeddings (reduces 250+ block, tract, county features below to 15 dimensions)
  ,coalesce(bg.block_component_pc1,-2.83159373436281) as block_component_pc1
  ,coalesce(bg.block_component_pc2,0.829194370075463) as block_component_pc2
  ,coalesce(bg.block_component_pc3,0.398877839631647) as block_component_pc3
  ,coalesce(bg.block_component_pc4,0.234574919827233) as block_component_pc4
  ,coalesce(bg.block_component_pc5,-0.034273927424433) as block_component_pc5
  
  ,coalesce(trc.tract_component_pc1,0.919401723259862) as tract_component_pc1
  ,coalesce(trc.tract_component_pc2,0.0290201188746687) as tract_component_pc2
  ,coalesce(trc.tract_component_pc3,-0.774097060714685) as tract_component_pc3
  ,coalesce(trc.tract_component_pc4,-0.292205220457999) as tract_component_pc4
  ,coalesce(trc.tract_component_pc5,0.135064039349648) as tract_component_pc5
  
  ,coalesce(cty.county_component_pc1,4.39902115601863) as county_component_pc1
  ,coalesce(cty.county_component_pc2,-2.76221417613783) as county_component_pc2
  ,coalesce(cty.county_component_pc3,0.642546411645455) as county_component_pc3
  ,coalesce(cty.county_component_pc4,0.4890816974394) as county_component_pc4
  ,coalesce(cty.county_component_pc5,-0.0771120869396656) as county_component_pc5
  
  -- Industry and economic composition (block group) from phoenix_census.acs_current
  ,coalesce(acs.xc_24070_e_10,10.5021808346473) as pct_prof_sci_mng
  ,coalesce(acs.xc_24070_e_11,23.3169387120635) as pct_educ_health
  ,coalesce(acs.xc_24070_e_12,9.76120640305069) as pct_arts_ent_hotel_food
  ,coalesce(acs.xc_24070_e_13,4.9372763541136) as pct_other_serv
  ,coalesce(acs.xc_24070_e_14,4.70981094468377) as pct_pub_admin
  ,coalesce(acs.xc_24070_e_15,67.6719088419246) as pct_private
  ,coalesce(acs.xc_24070_e_2,2.22482878526911) as pct_agriculture
  ,coalesce(acs.xc_24070_e_3,6.29887799760338) as pct_construction
  ,coalesce(acs.xc_24070_e_4,10.5900279493579) as pct_manufacturing
  ,coalesce(acs.xc_24070_e_6,11.5310261382819) as pct_retail
  ,coalesce(acs.xc_24070_e_7,5.05414222332237) as pct_transport_warehouse
  ,coalesce(acs.xc_24070_e_5,2.58820902288239) as pct_wholesale
  ,coalesce(acs.xc_24070_e_8,1.9808691752496) as pct_information
  ,coalesce(acs.xc_24070_e_9,6.15837855795037) as pct_finance_insur
  ,coalesce(acs.xc_24070_e_29,3.35201406323088) as pct_selfemployed_incorporated_biz
  ,coalesce(acs.xc_24070_e_43,8.18953615208745) as pct_not_for_profit
  ,coalesce(acs.xc_24070_e_57,14.1523126127464) as pct_government_worker
  ,coalesce(acs.xc_24070_e_71,6.28800827128944) as pct_selfemployed_nonincorporated_biz

  -- Language spoken at home (block group) from phoenix_census.acs_current
  ,coalesce(acs.xc_16001_e_2,80.2671499610826) as pct_lang_at_home_only_english
  ,coalesce(acs.xc_16001_e_3,12.0587109495928) as pct_lang_at_home_spanish
  ,coalesce(acs.xc_16001_e_15,1.65284907167414) as pct_lang_at_home_indo_euro
  ,coalesce(acs.xc_16001_e_33,0.344957984728002) as pct_lang_at_home_arabic
  ,coalesce(acs.xc_16001_e_21,0.96546189141353) as pct_lang_at_home_chinese
  ,coalesce(acs.xc_16001_e_30,0.828565265109512) as pct_lang_at_home_other_asian
  ,coalesce(acs.xc_16001_e_12,0.666356436745457) as pct_lang_at_home_russian_slavic
  /*
  ,coalesce(acs.xc_16001_e_6,0.65794864225899) as pct_lang_at_home_french
  ,coalesce(acs.xc_16001_e_18,0.322285583986244) as pct_lang_at_home_korean
  ,coalesce(acs.xc_16001_e_24,0.407945143013321) as pct_lang_at_home_vietnamese
  ,coalesce(acs.xc_16001_e_27,0.47831653476469) as pct_lang_at_home_tagalog
  ,coalesce(acs.xc_16001_e_36,0.641450896064653) as pct_lang_at_home_other_lang
  ,coalesce(acs.xc_16001_e_9,0.483782984689971) as pct_lang_at_home_germanic
  */

  -- Change of address (block group) from phoenix_census.acs_current
  ,coalesce(acs.xb_07001_e_81,0.594762297133181) as pct_moved_from_abroad
  ,coalesce(acs.xb_07001_e_49,3.09860403448412) as pct_moved_from_different_county_within_same_state
  ,coalesce(acs.xb_07001_e_65,2.21059028553814) as pct_moved_from_different_state
  ,coalesce(acs.xb_07001_e_33,8.71861633001596) as pct_moved_within_same_county
  ,coalesce(acs.xb_07001_e_17,85.1532493413606) as pct_same_house_1_year_ago

  -- Foreign born (block group) from phoenix_census.acs_current
  ,coalesce(acs.xb_05007_e_15,27.5581932832236) as pct_foreign_born_asia
  ,coalesce(acs.xb_05007_e_2,19.0890989707642) as pct_foreign_born_europe
  ,coalesce(acs.xb_05007_e_28,41.1432239245551) as pct_foreign_born_latin_america
  ,coalesce(acs.xb_05007_e_29,14.8401338549315) as pct_foreign_born_caribbean
  ,coalesce(acs.xb_05007_e_42,54.2330559167257) as pct_foreign_born_central_america
  ,coalesce(acs.xb_05007_e_43,51.6578188460353) as pct_foreign_born_mexico
  ,coalesce(acs.xb_05007_e_56,22.4681717297962) as pct_foreign_born_other_central_america
  ,coalesce(acs.xb_05007_e_69,17.2102764107977) as pct_foreign_born_south_america
  --,coalesce(acs.xb_05007_e_82,10.1881488482489) as pct_foreign_born_other_areas

  -- Demographic age, race, education quantities (block group) from Census 
  -- Source: https://www.census.gov/topics/research/guidance/planning-databases/2019.html
  ,coalesce(acs.xb_13008_e_2,5.28599158612465) as pct_had_birth_in_last_year
  ,coalesce(bg.pop_under_5_acs_13_17,126) as pop_under_5_acs_13_17
  ,coalesce(bg.pop_5_17_acs_13_17,353) as pop_5_17_acs_13_17
  ,coalesce(bg.pop_18_24_acs_13_17,186) as pop_18_24_acs_13_17
  ,coalesce(bg.pop_25_44_acs_13_17,539) as pop_25_44_acs_13_17
  ,coalesce(bg.pop_45_64_acs_13_17,526) as pop_45_64_acs_13_17
  ,coalesce(bg.pop_65plus_acs_13_17,295) as pop_65plus_acs_13_17
  ,coalesce(bg.hispanic_acs_13_17,324) as hispanic_acs_13_17
  ,coalesce(bg.nh_white_alone_acs_13_17,1281) as nh_white_alone_acs_13_17
  ,coalesce(bg.nh_blk_alone_acs_13_17,238) as nh_blk_alone_acs_13_17
  ,coalesce(bg.college_acs_13_17,480) as college_acs_13_17
  ,coalesce(bg.tot_prns_in_hhd_acs_13_17,1990) as tot_prns_in_hhd_acs_13_17
  
  -- Demographic age, race, education percentages (block group) from Census 
  ,coalesce(bg.pct_hispanic_acs_13_17,14.2960721578394) as pct_hispanic_acs_13_17
  ,coalesce(bg.pct_nh_white_alone_acs_13_17,65.5333628958917) as pct_nh_white_alone_acs_13_17
  ,coalesce(bg.pct_nh_blk_alone_acs_13_17,12.0509472949503) as pct_nh_blk_alone_acs_13_17
  ,coalesce(bg.pct_nh_aian_alone_acs_13_17,0.579797434159419) as pct_nh_aian_alone_acs_13_17
  ,coalesce(bg.pct_nh_asian_alone_acs_13_17,4.89668018767149) as pct_nh_asian_alone_acs_13_17
  ,coalesce(bg.pct_nh_nhopi_alone_acs_13_17,0.132654528723658) as pct_nh_nhopi_alone_acs_13_17
  ,coalesce(bg.pct_nh_sor_alone_acs_13_17,0.212287286270046) as pct_nh_sor_alone_acs_13_17
  ,coalesce(bg.pct_pop_5yrs_over_acs_13_17,94.1220723999255) as pct_pop_5yrs_over_acs_13_17
  ,coalesce(bg.pct_othr_lang_acs_13_17,18.1678419982226) as pct_othr_lang_acs_13_17
  ,coalesce(bg.pct_pop_25yrs_over_acs_13_17,68.8434220457763) as pct_pop_25yrs_over_acs_13_17
  ,coalesce(bg.pct_not_hs_grad_acs_13_17,11.3517254717505) as pct_not_hs_grad_acs_13_17
  ,coalesce(bg.pct_college_acs_13_17,32.7026963835036) as pct_college_acs_13_17

   --Income, poverty, home value, health insurance, public assistance (block group) from Census  
  ,coalesce(bg.med_hhd_inc_bg_acs_13_17,68045) as med_hhd_inc_bg_acs_13_17
  ,coalesce(bg.aggregate_hh_inc_acs_13_17,66425576) as aggregate_hh_inc_acs_13_17
  ,coalesce(bg.med_house_value_bg_acs_13_17,251049) as med_house_value_bg_acs_13_17
  ,coalesce(bg.aggr_house_value_acs_13_17,142357778) as aggr_house_value_acs_13_17
  ,coalesce(bg.pct_pov_univ_acs_13_17,98.2608273235828) as pct_pov_univ_acs_13_17
  ,coalesce(bg.pct_prs_blw_pov_lev_acs_13_17,13.4248472146267) as pct_prs_blw_pov_lev_acs_13_17
  ,coalesce(bg.pct_no_health_ins_acs_13_17,9.31498551549904) as pct_no_health_ins_acs_13_17
  ,coalesce(bg.pct_pub_asst_inc_acs_13_17,2.40123700682686) as pct_pub_asst_inc_acs_13_17
  ,coalesce(bg.avg_agg_hh_inc_acs_13_17,85659) as avg_agg_hh_inc_acs_13_17
  ,coalesce(bg.pct_no_plumb_acs_13_17,1.77677765652254) as pct_no_plumb_acs_13_17
  ,coalesce(bg.avg_agg_house_value_acs_13_17,187575) as avg_agg_house_value_acs_13_17

  -- Contactibility measures (block and tract) from Census 
  ,coalesce(bg.mail_return_rate_cen_2010,79.5546348682809) as mail_return_rate_cen_2010
  ,coalesce(bg.low_response_score,18.6366616406632) as low_response_score
  ,coalesce(trc.mail_return_rate_cen_2010_tract,79.8577954947941) as mail_return_rate_cen_2010_tract
  ,coalesce(trc.low_response_score_tract,19.8377285675556) as low_response_score_tract
  ,coalesce(trc.self_response_rate_acs_13_17_tract,63.7345458651875) as self_response_rate_acs_13_17_tract

  -- Phone, mobile, broadband, computer access (block and tract) from Census 
  ,coalesce(bg.pct_no_ph_srvc_acs_13_17,2.17084304611568) as pct_no_ph_srvc_acs_13_17
  ,coalesce(bg.pct_diff_hu_1yr_ago_acs_13_17,14.0637516667325) as pct_diff_hu_1yr_ago_acs_13_17
  ,coalesce(trc.pct_hhd_nocompdevic_acs_13_17_tract,12.5549686617213) as pct_hhd_nocompdevic_acs_13_17_tract
  ,coalesce(trc.pct_hhd_w_computer_acs_13_17_tract,78.8041191804697) as pct_hhd_w_computer_acs_13_17_tract
  ,coalesce(trc.pct_hhd_w_onlysphne_acs_13_17_tract,3.77286671999414) as pct_hhd_w_onlysphne_acs_13_17_tract
  ,coalesce(trc.pct_hhd_no_internet_acs_13_17_tract,17.1918159627054) as pct_hhd_no_internet_acs_13_17_tract
  ,coalesce(trc.pct_hhd_w_broadband_acs_13_17_tract,67.6080443696615) as pct_hhd_w_broadband_acs_13_17_tract
  ,coalesce(trc.pct_pop_nocompdevic_acs_13_17_tract,8.56910770409689) as pct_pop_nocompdevic_acs_13_17_tract
  ,coalesce(trc.pct_pop_w_broadcomp_acs_13_17_tract,82.53493757977) as pct_pop_w_broadcomp_acs_13_17_tract

  --Language spoken at home (block group) from Census 
  ,coalesce(bg.pct_eng_vw_span_acs_13_17,2.25840224846234) as pct_eng_vw_span_acs_13_17
  ,coalesce(bg.pct_eng_vw_indoeuro_acs_13_17,0.610076091630018) as pct_eng_vw_indoeuro_acs_13_17
  ,coalesce(bg.pct_eng_vw_api_acs_13_17,0.817020607263466) as pct_eng_vw_api_acs_13_17
  ,coalesce(bg.pct_eng_vw_acs_13_17,3.84689136566416) as pct_eng_vw_acs_13_17

  -- Household relations, marriage, children (block group) from Census 
  ,coalesce(bg.pct_rel_family_hhd_acs_13_17,67.4416347001227) as pct_rel_family_hhd_acs_13_17
  ,coalesce(bg.pct_mrdcple_hhd_acs_13_17,50.4965271841652) as pct_mrdcple_hhd_acs_13_17
  ,coalesce(bg.avg_tot_prns_in_hhd_acs_13_17,2.66520630002142) as avg_tot_prns_in_hhd_acs_13_17
  ,coalesce(bg.pct_rel_under_6_acs_13_17,19.79222392925) as pct_rel_under_6_acs_13_17
  ,coalesce(bg.pct_hhd_ppl_und_18_acs_13_17,32.0634267116686) as pct_hhd_ppl_und_18_acs_13_17
  
  -- Living not married, single women, not family, single parent, moved (block group) from Census 
  ,coalesce(bg.pct_not_mrdcple_hhd_acs_13_17,49.4127305321737) as pct_not_mrdcple_hhd_acs_13_17
  ,coalesce(bg.pct_female_no_hb_acs_13_17,12.2731174327349) as pct_female_no_hb_acs_13_17
  ,coalesce(bg.pct_nonfamily_hhd_acs_13_17,32.4676233732405) as pct_nonfamily_hhd_acs_13_17
  ,coalesce(bg.pct_sngl_prns_hhd_acs_13_17,26.2937187836754) as pct_sngl_prns_hhd_acs_13_17
  ,coalesce(bg.pct_hhd_moved_in_acs_13_17,40.407480726983) as pct_hhd_moved_in_acs_13_17
  
  -- Occupancy, vacancy (block group) from Census 
  ,coalesce(bg.pct_tot_occp_units_acs_13_17,89.6550041580204) as pct_tot_occp_units_acs_13_17
  ,coalesce(bg.pct_recent_built_hu_acs_13_17,0.995234668985221) as pct_recent_built_hu_acs_13_17
  ,coalesce(bg.pct_vacant_units_acs_13_17,10.2576800990454) as pct_vacant_units_acs_13_17
  
  -- Renter / owner (block group) from Census 
  ,coalesce(bg.pct_renter_occp_hu_acs_13_17,32.5914873973869) as pct_renter_occp_hu_acs_13_17
  ,coalesce(bg.pct_owner_occp_hu_acs_13_17,67.3177701390769) as pct_owner_occp_hu_acs_13_17
  
  -- Single / multiunit, multiperson rooms, mobile home (block group) from Census 
  ,coalesce(bg.pct_single_unit_acs_13_17,71.626622643208) as pct_single_unit_acs_13_17
  ,coalesce(bg.pct_mlt_u2_9_strc_acs_13_17,11.4387903065272) as pct_mlt_u2_9_strc_acs_13_17
  ,coalesce(bg.pct_mlt_u10p_acs_13_17,11.1029455588377) as pct_mlt_u10p_acs_13_17
  ,coalesce(bg.pct_crowd_occp_u_acs_13_17,2.9544471103295) as pct_crowd_occp_u_acs_13_17
  ,coalesce(bg.pct_mobile_homes_acs_13_17,5.66837761256287) as pct_mobile_homes_acs_13_17 
 
  -- Population age, race, education, children (tract) from Census 
  ,coalesce(trc.tot_population_acs_13_17_tract,5465) as tot_population_acs_13_17_tract
  ,coalesce(trc.median_age_acs_13_17_tract,39.7290404593366) as median_age_acs_13_17_tract
  ,coalesce(trc.civ_noninst_pop_acs_13_17_tract,5400) as civ_noninst_pop_acs_13_17_tract
  ,coalesce(trc.tot_prns_in_hhd_acs_13_17_tract,5358) as tot_prns_in_hhd_acs_13_17_tract
  ,coalesce(trc.pct_schl_enroll_3_4_acs_13_17_tract,48.8100875693337) as pct_schl_enroll_3_4_acs_13_17_tract
  ,coalesce(trc.avg_tot_prns_in_hhd_acs_13_17_tract,2.61999195050279) as avg_tot_prns_in_hhd_acs_13_17_tract
  ,coalesce(trc.pct_mrdcple_w_child_acs_13_17_tract,41.4087852007455) as pct_mrdcple_w_child_acs_13_17_tract
  ,coalesce(trc.pct_college_acs_13_17_tract,32.1265139931656) as pct_college_acs_13_17_tract
  
  -- Income, home value, poverty, health insurance, public assistance, disability (tract) from Census 
  ,coalesce(trc.med_hhd_inc_acs_13_17_tract,65326.8763985767) as med_hhd_inc_acs_13_17_tract
  ,coalesce(trc.aggregate_hh_inc_acs_13_17_tract,174829609.633088) as aggregate_hh_inc_acs_13_17_tract
  ,coalesce(trc.med_house_value_acs_13_17_tract,225182.009954215) as med_house_value_acs_13_17_tract
  ,coalesce(trc.pct_prs_blw_pov_lev_acs_13_17_tract,13.7351624595932) as pct_prs_blw_pov_lev_acs_13_17_tract
  ,coalesce(trc.pct_no_health_ins_acs_13_17_tract,9.50153871512217) as pct_no_health_ins_acs_13_17_tract
  ,coalesce(trc.pct_civ_unemp_16p_acs_13_17_tract,6.41159512780591) as pct_civ_unemp_16p_acs_13_17_tract
  ,coalesce(trc.pct_pop_disabled_acs_13_17_tract,12.8469866347057) as pct_pop_disabled_acs_13_17_tract
  ,coalesce(trc.pct_children_in_pov_acs_13_17_tract,17.955219420835) as pct_children_in_pov_acs_13_17_tract
  ,coalesce(trc.pct_nohealthins_u19_acs_13_17_tract,5.20173238270458) as pct_nohealthins_u19_acs_13_17_tract
  ,coalesce(trc.pct_nohealthins1964_acs_13_17_tract,13.639066910752) as pct_nohealthins1964_acs_13_17_tract
  ,coalesce(trc.pct_nohealthins_65p_acs_13_17_tract,0.922277158371395) as pct_nohealthins_65p_acs_13_17_tract
  ,coalesce(trc.pct_pub_asst_inc_acs_13_17_tract,2.33454496030974) as pct_pub_asst_inc_acs_13_17_tract
  ,coalesce(trc.avg_agg_hh_inc_acs_13_17_tract,82921) as avg_agg_hh_inc_acs_13_17_tract
  ,coalesce(trc.avg_agg_house_value_acs_13_17_tract,171676.234079983) as avg_agg_house_value_acs_13_17_tract

  /* Other block group covariates from Census 
  ,coalesce(bg.males_acs_13_17,993) as males_acs_13_17
  ,coalesce(bg.females_acs_13_17,1035) as females_acs_13_17
  ,coalesce(bg.median_age_acs_13_17,40.2860906050171) as median_age_acs_13_17
  ,coalesce(bg.nh_aian_alone_acs_13_17,10) as nh_aian_alone_acs_13_17
  ,coalesce(bg.nh_asian_alone_acs_13_17,119) as nh_asian_alone_acs_13_17
  ,coalesce(bg.nh_nhopi_alone_acs_13_17,2) as nh_nhopi_alone_acs_13_17
  ,coalesce(bg.nh_sor_alone_acs_13_17,4) as nh_sor_alone_acs_13_17
  ,coalesce(bg.pop_5yrs_over_acs_13_17,1901) as pop_5yrs_over_acs_13_17
  ,coalesce(bg.othr_lang_acs_13_17,382) as othr_lang_acs_13_17
  ,coalesce(bg.pop_25yrs_over_acs_13_17,1362) as pop_25yrs_over_acs_13_17
  ,coalesce(bg.not_hs_grad_acs_13_17,139) as not_hs_grad_acs_13_17
  ,coalesce(bg.pov_univ_acs_13_17,1990) as pov_univ_acs_13_17
  ,coalesce(bg.prs_blw_pov_lev_acs_13_17,237) as prs_blw_pov_lev_acs_13_17
  ,coalesce(bg.one_health_ins_acs_13_17,1507) as one_health_ins_acs_13_17
  ,coalesce(bg.two_plus_health_ins_acs_13_17,313) as two_plus_health_ins_acs_13_17
  ,coalesce(bg.no_health_ins_acs_13_17,185) as no_health_ins_acs_13_17
  ,coalesce(bg.pop_1yr_over_acs_13_17,2004) as pop_1yr_over_acs_13_17
  ,coalesce(bg.diff_hu_1yr_ago_acs_13_17,291) as diff_hu_1yr_ago_acs_13_17
  ,coalesce(bg.eng_vw_span_acs_13_17,15) as eng_vw_span_acs_13_17
  ,coalesce(bg.eng_vw_indo_euro_acs_13_17,4) as eng_vw_indo_euro_acs_13_17
  ,coalesce(bg.eng_vw_api_acs_13_17,6) as eng_vw_api_acs_13_17
  ,coalesce(bg.eng_vw_other_acs_13_17,1) as eng_vw_other_acs_13_17
  ,coalesce(bg.eng_vw_acs_13_17,27) as eng_vw_acs_13_17
  ,coalesce(bg.rel_family_hhd_acs_13_17,507) as rel_family_hhd_acs_13_17
  ,coalesce(bg.mrdcple_fmly_hhd_acs_13_17,388) as mrdcple_fmly_hhd_acs_13_17
  ,coalesce(bg.not_mrdcple_hhd_acs_13_17,353) as not_mrdcple_hhd_acs_13_17
  ,coalesce(bg.female_no_hb_acs_13_17,85) as female_no_hb_acs_13_17
  ,coalesce(bg.nonfamily_hhd_acs_13_17,235) as nonfamily_hhd_acs_13_17
  ,coalesce(bg.sngl_prns_hhd_acs_13_17,189) as sngl_prns_hhd_acs_13_17
  ,coalesce(bg.hhd_ppl_und_18_acs_13_17,247) as hhd_ppl_und_18_acs_13_17
  ,coalesce(bg.rel_child_under_6_acs_13_17,103) as rel_child_under_6_acs_13_17
  ,coalesce(bg.hhd_moved_in_acs_13_17,323) as hhd_moved_in_acs_13_17
  ,coalesce(bg.pub_asst_inc_acs_13_17,15) as pub_asst_inc_acs_13_17
  ,coalesce(bg.med_hhd_inc_tr_acs_13_17,66697) as med_hhd_inc_tr_acs_13_17
  ,coalesce(bg.tot_housing_units_acs_13_17,831) as tot_housing_units_acs_13_17
  ,coalesce(bg.tot_occp_units_acs_13_17,742) as tot_occp_units_acs_13_17
  ,coalesce(bg.tot_vacant_units_acs_13_17,89) as tot_vacant_units_acs_13_17
  ,coalesce(bg.renter_occp_hu_acs_13_17,239) as renter_occp_hu_acs_13_17
  ,coalesce(bg.owner_occp_hu_acs_13_17,502) as owner_occp_hu_acs_13_17
  ,coalesce(bg.single_unit_acs_13_17,589) as single_unit_acs_13_17
  ,coalesce(bg.mlt_u2_9_strc_acs_13_17,86) as mlt_u2_9_strc_acs_13_17
  ,coalesce(bg.mlt_u10p_acs_13_17,109) as mlt_u10p_acs_13_17
  ,coalesce(bg.mobile_homes_acs_13_17,45) as mobile_homes_acs_13_17
  ,coalesce(bg.crowd_occp_u_acs_13_17,20) as crowd_occp_u_acs_13_17
  ,coalesce(bg.occp_u_no_ph_srvc_acs_13_17,15) as occp_u_no_ph_srvc_acs_13_17
  ,coalesce(bg.no_plumb_acs_13_17,12) as no_plumb_acs_13_17
  ,coalesce(bg.recent_built_hu_acs_13_17,13) as recent_built_hu_acs_13_17
  ,coalesce(bg.med_house_value_tr_acs_13_17,255420) as med_house_value_tr_acs_13_17
  ,coalesce(bg.pct_one_health_ins_acs_13_17,73.0312943978964) as pct_one_health_ins_acs_13_17
  ,coalesce(bg.pct_twophealthins_acs_13_17,16.6232477562543) as pct_twophealthins_acs_13_17
  ,coalesce(bg.pct_pop_1yr_over_acs_13_17,98.8720575531394) as pct_pop_1yr_over_acs_13_17
  ,coalesce(bg.pct_eng_vw_other_acs_13_17,0.161400892051139) as pct_eng_vw_other_acs_13_17
  */
  
  -- Neighborhood effects and determinants of socioeconomic mobility (tract)
  -- Source: http://www.equality-of-opportunity.org/data/
  ,coalesce(hhinc_mean2000,84608.8675030326) as hhinc_mean2000
  ,coalesce(mean_commutetime2000,27.1511583146669) as mean_commutetime2000
  ,coalesce(frac_coll_plus2010,0.293429982272959) as frac_coll_plus2010
  ,coalesce(frac_coll_plus2000,0.257373987649339) as frac_coll_plus2000
  ,coalesce(foreign_share2010,0.100118031983283) as foreign_share2010
  ,coalesce(gsmn_math_g3_2013,3.37572107873095) as gsmn_math_g3_2013
  ,coalesce(rent_twobed2015,966) as rent_twobed2015
  ,coalesce(singleparent_share2010,0.309102713069427) as singleparent_share2010
  ,coalesce(singleparent_share1990,0.206551891336751) as singleparent_share1990
  ,coalesce(singleparent_share2000,0.271247529384927) as singleparent_share2000
  ,coalesce(traveltime15_2010,0.285819197356473) as traveltime15_2010
  ,coalesce(mail_return_rate2010,80.0162608373062) as mail_return_rate2010
  ,coalesce(ln_wage_growth_hs_grad,0.0403462424119161) as ln_wage_growth_hs_grad
  ,coalesce(jobs_total_5mi_2015,103216) as jobs_total_5mi_2015
  ,coalesce(jobs_highpay_5mi_2015,54903) as jobs_highpay_5mi_2015
  ,coalesce(popdensity2010,4823.57062538955) as popdensity2010
  ,coalesce(ann_avg_job_growth_2004_2013,0.0181115701144405) as ann_avg_job_growth_2004_2013
  ,coalesce(job_density_2013,1984.48679821812) as job_density_2013

  -- Mean household income rank for children whose parents were at the 25th percentile of the national income distribution (tract)
  ,coalesce(kfr_black_female_p25,0.382206283736095) as kfr_black_female_p25
  ,coalesce(kfr_hisp_female_p25,0.447209772710046) as kfr_hisp_female_p25
  ,coalesce(kfr_white_female_p25,0.478709529962811) as kfr_white_female_p25
  ,coalesce(kfr_black_male_p25,0.334165703430485) as kfr_black_male_p25
  ,coalesce(kfr_hisp_male_p25,0.426604211724126) as kfr_hisp_male_p25
  ,coalesce(kfr_white_male_p25,0.451330839800494) as kfr_white_male_p25
  
  -- Fraction of children born in 1978-1983 birth cohorts with parents at the 25th percentile of the national income distribution who were incarcerated on April 2010 (tract)
  ,coalesce(jail_black_female_p25,0.00631471659606834) as jail_black_female_p25
  ,coalesce(jail_hisp_female_p25,0.00310764332757126) as jail_hisp_female_p25
  ,coalesce(jail_white_female_p25,0.00464152872385685) as jail_white_female_p25
  ,coalesce(jail_black_male_p25,0.0892361528008483) as jail_black_male_p25
  ,coalesce(jail_hisp_male_p25,0.0327939162010329) as jail_hisp_male_p25
  ,coalesce(jail_white_male_p25,0.0300551823879225) as jail_white_male_p25
   /*
  ,coalesce(kfr_pooled_pooled_p25,0.433407375913507) as kfr_pooled_pooled_p25
  ,coalesce(jail_pooled_pooled_p25,0.0213830856711985) as jail_pooled_pooled_p25
  ,coalesce(kfr_black_pooled_p25,0.35918603467439) as kfr_black_pooled_p25
  ,coalesce(kfr_hisp_pooled_p25,0.437374508308422) as kfr_hisp_pooled_p25
  ,coalesce(kfr_white_pooled_p25,0.464605602599441) as kfr_white_pooled_p25
  ,coalesce(jail_black_pooled_p25,0.0446806953406881) as jail_black_pooled_p25
  ,coalesce(jail_hisp_pooled_p25,0.0154079826628898) as jail_hisp_pooled_p25
  ,coalesce(jail_white_pooled_p25,0.0168824963959682) as jail_white_pooled_p25
  ,coalesce(kfr_pooled_female_p25,0.450062288593881) as kfr_pooled_female_p25
  ,coalesce(kfr_pooled_male_p25,0.417754000394223) as kfr_pooled_male_p25
  ,coalesce(jail_pooled_female_p25,0.00385537099132036) as jail_pooled_female_p25
  ,coalesce(jail_pooled_male_p25,0.0403304433992196) as jail_pooled_male_p25
  */
 
  -- Socioeconomic mobility (county)
  ,coalesce(cty.cty_exposure_natl_hh_income_at_age_26_p25,-0.00116575193502995) as cty_exposure_natl_hh_income_at_age_26_p25
  ,coalesce(cty.cty_exposure_effects_natl_hh_income_at_age_26_p75,-0.00914190872321934) as cty_exposure_effects_natl_hh_income_at_age_26_p75
  ,coalesce(cty.cty_exposure_college_attendance_at_ages_18_23_p25,-0.0106601971605495) as cty_exposure_college_attendance_at_ages_18_23_p25
  ,coalesce(cty.cty_exposure_college_attendance_at_ages_18_23_p75,0.00404288046049372) as cty_exposure_college_attendance_at_ages_18_23_p75
  ,coalesce(cty.cty_exposure_value_of_hh_income_at_age_26_p25,-1.6164728901619) as cty_exposure_value_of_hh_income_at_age_26_p25
  ,coalesce(cty.cty_exposure_value_of_hh_income_at_age_26_p75,-2.39628461786069) as cty_exposure_value_of_hh_income_at_age_26_p75
  ,coalesce(cty.cty_exposure_marriage_rate_at_age_26_p25,0.00831731094751134) as cty_exposure_marriage_rate_at_age_26_p25
  ,coalesce(cty.cty_exposure_marriage_rate_at_age_26_p75,-0.00209350639976409) as cty_exposure_marriage_rate_at_age_26_p75
  
  -- Segregation, inequality, precarity (county)
  ,coalesce(cty.racial_segregation,0.171863755227516) as racial_segregation
  ,coalesce(cty.income_segregation,0.0711031029332531) as income_segregation
  ,coalesce(cty.segregation_of_poverty_p25,0.0632798063417) as segregation_of_poverty_p25
  ,coalesce(cty.segregation_of_poverty_p75,0.0773876799842774) as segregation_of_poverty_p75
  ,coalesce(cty.gini,0.445165973890533) as gini
  ,coalesce(cty.top_1pct_income_share,0.00136530467373247) as top_1pct_income_share
  ,coalesce(cty.fraction_middle_class_between_p25_and_p75,0.498556746393662) as fraction_middle_class_between_p25_and_p75
  --,coalesce(cty.segregation_of_affluence_p75,-0.00107713826175978) as segregation_of_affluence_p75
  ,coalesce(cty.unemployment_rate,0.0467524233687776) as unemployment_rate
  ,coalesce(cty.fraction_black,-0.0583997544016713) as fraction_black
  ,coalesce(cty.poverty_rate,-0.0614342101400683) as poverty_rate
  ,coalesce(cty.fraction_of_children_with_single_mothers,0.220423024748469) as fraction_of_children_with_single_mothers
  ,coalesce(cty.fraction_of_adults_divorced,0.0973050643676738) as fraction_of_adults_divorced
  --,coalesce(cty.fraction_of_adults_married,0.549051926141323) as fraction_of_adults_married
  /* Inequality (county)
  ,coalesce(cty.fraction_of_parents_in_1st_national_income_decile,0.0592356556168324) as fraction_of_parents_in_1st_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_2nd_national_income_decile,0.0823170255208072) as fraction_of_parents_in_2nd_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_3rd_national_income_decile,0.0904236007944681) as fraction_of_parents_in_3rd_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_4th_national_income_decile,0.0934512150370768) as fraction_of_parents_in_4th_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_5th_national_income_decile,0.0980693805411928) as fraction_of_parents_in_5th_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_6th_national_income_decile,0.10558186962211) as fraction_of_parents_in_6th_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_7th_national_income_decile,0.113377997998127) as fraction_of_parents_in_7th_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_8th_national_income_decile,0.118968681619563) as fraction_of_parents_in_8th_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_9th_national_income_decile,0.120493228521394) as fraction_of_parents_in_9th_national_income_decile
  ,coalesce(cty.fraction_of_parents_in_10th_national_income_decile,0.117994046920011) as fraction_of_parents_in_10th_national_income_decile
  */
  
  -- Urban area statistics (county)
  ,coalesce(cty.log_population_density,6.12619867247722) as log_population_density
  ,coalesce(cty.fraction_with_commute_less15_mins,0.294810198323762) as fraction_with_commute_less15_mins
  --,coalesce(cty.fraction_with_commute__15_mins,-0.0099986728359157) as fraction_with_commute__15_mins
  ,coalesce(cty.labor_force_participation,0.646471359028166) as labor_force_participation
  ,coalesce(cty.share_working_in_manufacturing,0.140205520832831) as share_working_in_manufacturing
  ,coalesce(cty.teenage_14_16_labor_force_participation,0.452254845437077) as teenage_14_16_labor_force_participation
  ,coalesce(cty.migration_inflow_rate,0.0402778500933353) as migration_inflow_rate
  ,coalesce(cty.migration_outlflow_rate,0.0376845754615703) as migration_outlflow_rate
  ,coalesce(cty.fraction_foreign_born,0.0885283698594889) as fraction_foreign_born

  -- Public investment (county)
  ,coalesce(cty.local_tax_rate,0.0245131597256442) as local_tax_rate
  ,coalesce(cty.local_tax_rate_per_capita,0.874316688348967) as local_tax_rate_per_capita
  ,coalesce(cty.local_govt_expenditures_per_capita,2.38913995258842) as local_govt_expenditures_per_capita
  ,coalesce(cty.state_eitc_exposure,1.9312584869822) as state_eitc_exposure
  ,coalesce(cty.tax_progressivity,0.700113017799344) as tax_progressivity
  ,coalesce(cty.school_expenditure_per_student,6.56856733641607) as school_expenditure_per_student
  ,coalesce(cty.student_teacher_ratio,17.9089800964499) as student_teacher_ratio
  ,coalesce(cty.test_score_percentile_income_adjusted,-3.42893630380351) as test_score_percentile_income_adjusted
  ,coalesce(cty.high_school_dropout_rate_income_adjusted,0.011010379642501) as high_school_dropout_rate_income_adjusted
  ,coalesce(cty.number_of_colleges_per_capita,0.0127898836217141) as number_of_colleges_per_capita
  ,coalesce(cty.college_tuition,5480.21990120936) as college_tuition
  ,coalesce(cty.college_graduation_rate_income_adjusted,0.00744872225522498) as college_graduation_rate_income_adjusted
  ,coalesce(cty.no_college,0) as no_college
  
  -- Affordability (county)
  ,coalesce(cty.median_house_price_for_below_median_income_families,111719.45713786) as median_house_price_for_below_median_income_families
  ,coalesce(cty.median_house_price_for_above_median_income_families,140790.018749115) as median_house_price_for_above_median_income_families
  ,coalesce(cty.median_house_rent_for_below_median_income_families,592.904223753325) as median_house_rent_for_below_median_income_families
  ,coalesce(cty.median_house_rent_for_above_median_income_families,669.260216430718) as median_house_rent_for_above_median_income_families
  ,coalesce(cty.household_income_per_capita,39765.3684302678) as household_income_per_capita
  ,coalesce(cty.location_affordability_of_very_low_income_individual,148.729604991793) as location_affordability_of_very_low_income_individual
  ,coalesce(cty.location_affordability_of_median_income_family,53.639155643285) as location_affordability_of_median_income_family
  --,coalesce(cty.location_affordability_of_median_household,0.0156450285786106) as location_affordability_of_median_household

  -- Social cohesion (county)
  ,coalesce(cty.social_capital_index,-0.32310404048882) as social_capital_index
  ,coalesce(cty.fraction_religious,0.503391600402358) as fraction_religious
  ,coalesce(cty.violent_crime_rate,0.00179317116792631) as violent_crime_rate
  ,coalesce(cty.total_crime_rate,0.00705892372461041) as total_crime_rate
  
  -- Whether the child reports positive W-2 earnings with income quintile and gender (county)
  ,coalesce(cty.w2_pos_30_q1_f,0.728292494647528) as w2_pos_30_q1_f
  ,coalesce(cty.w2_pos_30_q1_m,0.696787968406929) as w2_pos_30_q1_m
  ,coalesce(cty.w2_pos_30_q2_f,0.774017520106013) as w2_pos_30_q2_f
  ,coalesce(cty.w2_pos_30_q2_m,0.772397705924151) as w2_pos_30_q2_m
  ,coalesce(cty.w2_pos_30_q3_f,0.800166013866464) as w2_pos_30_q3_f
  ,coalesce(cty.w2_pos_30_q3_m,0.825697611883081) as w2_pos_30_q3_m
  ,coalesce(cty.w2_pos_30_q4_f,0.827237745177449) as w2_pos_30_q4_f
  ,coalesce(cty.w2_pos_30_q4_m,0.860967714537158) as w2_pos_30_q4_m
  
  -- 2016 primary returns (county)
  ,coalesce(cty.primary16_clinton,0.5480444769384) as primary16_clinton
  ,coalesce(cty.primary16_sanders,0.434347726321799) as primary16_sanders
  
-- Individual Binaries
  -- Residential area
  ,case 
  when tc.ts_tsmart_urbanicity in ('U5', 'U6') then 1
  when p.voting_address_urbanicity in ('U5','U6') then 1
  when p.census_density_urban_area then 1
  else 0 end geo_urban
  ,case 
  when tc.ts_tsmart_urbanicity in ('S3', 'S4') then 1
  when p.voting_address_urbanicity in ('S3','S4') then 1
  when p.census_density_urban_cluster then 1
  else 0 end geo_suburban
  ,case 
  when tc.ts_tsmart_urbanicity in ('R1', 'R2') then 1
  when p.voting_address_urbanicity in ('R1','R2') then 1 
  when p.census_density_rural then 1
  else 0 end geo_rural

  -- Homeownership binaries
  ,case when tc.tb_homeowner_flg = 'N' then 1 
  else 0 end as tb_homeowner_flg_no
  ,case when tc.tb_homeowner_flg = 'Y' then 1 
  else 0 end as tb_homeowner_flg_yes
 
  -- Gender binaries
  ,case 
  when p.gender_combined = 'F' then 1
  when l2.voters_gender = 'F' then 1
  when tc.tb_gender = 'F' then 1
  when tc.xpg_ind_lvl_gender = 'F' then 1
  else 0 end as female
  ,case 
  when p.gender_combined = 'M' then 1
  when l2.voters_gender = 'M' then 1
  when tc.tb_gender = 'M' then 1
  when tc.xpg_ind_lvl_gender = 'M' then 1
  else 0 end as male
  
  -- Marital status binaries
  ,case when (as20.civis_2020_marriage > 0.5 and tc.ts_tsmart_marriage_score > 50 and right(tc.xpg_ind_lvl_marital_status,1) = 'M')
  and (coalesce(p.gender_combined, l2.voters_gender, tc.tb_gender, tc.xpg_ind_lvl_gender) = 'F') then 1 
  else 0 end as married_female
  ,case when (as20.civis_2020_marriage > 0.5 and tc.ts_tsmart_marriage_score > 50 and right(tc.xpg_ind_lvl_marital_status,1) = 'M')
  and (coalesce(p.gender_combined, l2.voters_gender, tc.tb_gender, tc.xpg_ind_lvl_gender) = 'M') then 1 
  else 0 end as married_male
  ,case when (as20.civis_2020_marriage > 0.5 and tc.ts_tsmart_marriage_score > 50 and right(tc.xpg_ind_lvl_marital_status,1) = 'M') then 1 
  else 0 end as married 

  -- Race and ethnicity binaries
  ,case 
  when as20.civis_2020_likely_race = 'W' then 1
  when p.ethnicity_combined = 'W' then 1
  when tc.ts_tsmart_p_white >= .66 then 1
  else 0 end as eth_white
  ,case 
  when as20.civis_2020_likely_race = 'A' then 1
  when p.ethnicity_combined = 'A' then 1
  when tc.ts_tsmart_p_asian >= .66 then 1 
  else 0 end as eth_asian
  ,case 
  when as20.civis_2020_likely_race = 'B' then 1
  when p.ethnicity_combined = 'B' then 1
  when tc.ts_tsmart_p_afam >= .66 then 1 
  else 0 end as eth_afam
  ,case 
  when as20.civis_2020_likely_race = 'H' then 1
  when p.ethnicity_combined = 'H' then 1 
  when tc.ts_tsmart_p_hisp >= .66 then 1 
  else 0 end as eth_hispanic
  ,case 
  when as20.civis_2020_likely_race = 'N' then 1
  when p.ethnicity_combined = 'N' then 1
  when tc.ts_tsmart_p_natam >= .66 then 1 
  else 0 end as eth_native

  -- Income binaries
  ,case
  when tc.xpg_estimated_household_income in ('A','B') then 1
  when as18.dnc_2018_income_dollars < 25000 then 1
  when bg.med_hhd_inc_bg_acs_13_17 < 25000 then 1
  else 0 end as income_0k_25k  
  ,case
  when tc.xpg_estimated_household_income in ('C','D') then 1
  when as18.dnc_2018_income_dollars >= 25000 and as18.dnc_2018_income_dollars < 50000 then 1
  --when bg.med_hhd_inc_bg_acs_13_17 >= 25000 and bg.med_hhd_inc_bg_acs_13_17 < 50000 then 1
  else 0 end as income_25k_50k  
  ,case 
  when tc.xpg_estimated_household_income in ('E','F') then 1
  when as18.dnc_2018_income_dollars >= 50000 and as18.dnc_2018_income_dollars < 100000 then 1
  --when bg.med_hhd_inc_bg_acs_13_17 >= 50000 and bg.med_hhd_inc_bg_acs_13_17 < 100000 then 1
  else 0 end as income_50k_100k  
  ,case 
  when tc.xpg_estimated_household_income in ('G','H') then 1
  when as18.dnc_2018_income_dollars >= 100000 and as18.dnc_2018_income_dollars < 150000 then 1
  --when bg.med_hhd_inc_bg_acs_13_17 >= 100000 and bg.med_hhd_inc_bg_acs_13_17 < 150000 then 1
  else 0 end as income_100k_150k  
  ,case
  when tc.xpg_estimated_household_income in ('I','J','K','L') then 1
  when as18.dnc_2018_income_dollars >= 150000 then 1
  when bg.med_hhd_inc_bg_acs_13_17 >= 200000 then 1
  else 0 end as income_150k_plus

  -- Age continuous and binaries
  ,coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) as age_continuous
  ,case 
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) between 18 and 22 then 1 else 0 end as age_18_22
  ,case  
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) between 23 and 29 then 1 else 0 end as age_23_29
  ,case  
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) between 30 and 39 then 1 else 0 end as age_30_39
  ,case  
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) between 40 and 49 then 1 else 0 end as age_40_49
  ,case  
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) between 50 and 59 then 1 else 0 end as age_50_59
  ,case  
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) between 60 and 69 then 1 else 0 end as age_60_69
  ,case  
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) between 70 and 79 then 1 else 0 end as age_70_79
  ,case  
  when coalesce(p.age_combined,l2.voters_age::int,tc.tb_age::int,tc.xpg_ind_lvl_exact_age::int,tc.xpg_ind_lvl_estimated_age::int) >= 80 then 1 else 0 end as age_80plus
    
  -- Education binaries
  ,case
  when tc.tb_education_cd in (3,4) then 1
  when as18.dnc_2018_college_graduate > .5 and tc.ts_tsmart_college_graduate_score > 50 then 1
  when as18.dnc_2018_college_graduate > .5 then 1
  when tc.ts_tsmart_college_graduate_score > 50 then 1
  when right(tc.xpg_ind_lvl_education_model,1) in ('3','4') and left(tc.xpg_ind_lvl_education_model,1) in ('1','2') then  1
  else 0 end as bach_or_higher
  ,case
  when as18.dnc_2018_college_graduate <= .5 and tc.ts_tsmart_high_school_only_score > 50 then 1
  when tc.tb_education_cd in (1,2) then 1
  when as18.dnc_2018_college_graduate <= .5 then 1
  when tc.ts_tsmart_high_school_only_score > 50 then 1
  when right(tc.xpg_ind_lvl_education_model,1) in ('1','2') and left(tc.xpg_ind_lvl_education_model,1) in ('1','2') then 1
  else 0 end as less_than_bach
    
  -- Party ID binaries
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Democratic' or party_id = 1 then 1 
  else 0 end as party_dem
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Republican' or party_id = 2 then 1 
  else 0 end as party_rep
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Other' or party_id = 3 then 1 
  else 0 end as party_other 
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Libertarian' or party_id = 4 then 1 
  else 0 end as party_libertarian 
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Green' or party_id = 5 then 1 
  else 0 end as party_green 
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Independent' or party_id = 6 then 1 
  else 0 end as party_independent 
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Nonpartisan' or party_id = 7 then 1 
  else 0 end as party_nopartypref
  ,case when coalesce(p.party_name_dnc,l2.parties_description) = 'Unaffiliated' or party_id = 8 then 1 
  else 0 end as party_unaffiliated
  ,case when pv.state_code in ('AL','GA','HI','IL','IN','MI','MN','MO','MS','MT','OH','SC','TN','TX','VA','VT','WA','WI') then 1 
  else 0 end as non_party_reg_state           
  
  -- Voter registration history binaries
  ,case  
  when registration_date::date > '2018-11-08' then 1 else 0 end as vote_new_reg
  ,case when p.reg_voter_flag then 1
  else 0 end as reg_voter_flag
  ,case when p.is_permanent_absentee then 1
  else 0 end as is_permanent_absentee
  ,case when p.years_registered = 1 then 1
  else 0 end as years_registered_1year
  ,case when p.years_registered IN (2,3) then 1
  else 0 end as years_registered_2_3years
  ,case when p.years_registered IN (4,5) then 1
  else 0 end as years_registered_4_5years
  ,case when p.years_registered IN (5,6,7,8,9) then 1
  else 0 end as years_registered_5_10years
  ,case when p.years_registered >=10 AND years_registered <25 then 1
  else 0 end as years_registered_10_25years
  ,case when p.years_registered >=25 then 1
  else 0 end as years_registered_25plusyears
  
  -- General election participation binaries
  ,pv.vote_g_2018
  ,pv.vote_g_2016
  ,pv.vote_g_2014
  ,pv.vote_g_2012
  
  -- Dem presidential primary participation binaries
  ,pv.vote_pp_2000_party_d
  ,pv.vote_pp_2004_party_d
  ,pv.vote_pp_2008_party_d
  ,pv.vote_pp_2016_party_d
  ,case 
  when (pv.vote_pp_2012 = 1 or pv.vote_pp_2016 = 1) then 1 
  else 0 end as pre_primary_voter_post12
  ,case 
  when (pv.vote_p_2016_party_d = 1 or
  pv.vote_p_2017_party_d = 1 or
  pv.vote_p_2018_party_d = 1) then 1 
  else 0 end as primary_dem_voter_post16
  ,case 
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
  pv.vote_p_2018_party_d = 1) then 1 
  else 0 end as primary_dem_voter_post08
 
  -- Early voter / absentee voter binaries
  ,case 
  when (pv.vote_pp_2016_method_early = 1 or
  pv.vote_p_2017_method_early = 1 or
  pv.vote_p_2018_method_early = 1) then 1 
  else 0 end as primary_early_voter_post16
  ,case 
  when (pv.vote_p_2014_method_absentee = 1 or
  pv.vote_p_2015_method_absentee = 1 or
  pv.vote_p_2016_method_absentee = 1 or
  pv.vote_p_2017_method_absentee = 1 or
  pv.vote_p_2018_method_absentee = 1) then 1 
  else 0 end as primary_absentee_voter_post14
  ,case 
  when (pv.vote_g_2014_method_absentee = 1 or
  pv.vote_g_2015_method_absentee = 1 or
  pv.vote_g_2016_method_absentee = 1 or
  pv.vote_g_2017_method_absentee = 1 or
  pv.vote_g_2018_method_absentee = 1) then 1 
  else 0 end as general_absentee_voter_post14
  ,case 
  when (pv.vote_pp_2004_method_absentee = 1 or
  pv.vote_pp_2008_method_absentee = 1 or
  pv.vote_pp_2012_method_absentee = 1 or
  pv.vote_pp_2016_method_absentee = 1) then 1 
  else 0 end as pres_primary_absentee_voter_post04
 
  -- Lapsed voter binaries
  ,case 
  when (pv.vote_pp_2008_novote_eligible = 1 and
  pv.vote_pp_2016_novote_eligible = 1) then 1 
  else 0 end as pres_primary_novote_eligible_post08
  ,case 
  when (pv.vote_p_2016_novote_eligible = 1 and
  pv.vote_p_2018_novote_eligible = 1) then 1 
  else 0 end as primary_novote_eligible_post16
  ,case 
  when (pv.vote_g_2008_novote_eligible = 1 and 
  pv.vote_g_2012_novote_eligible = 1 and
  pv.vote_g_2016_novote_eligible = 1 and
  pv.vote_g_2018_novote_eligible = 1) then 1 
  else 0 end as general_novote_eligible_post08
 
  -- Voting frequency binaries
  ,case 
  when (pv.vote_g_2008 +
  pv.vote_g_2012 +
  pv.vote_g_2013 +
  pv.vote_g_2014 +
  pv.vote_g_2015 +
  pv.vote_g_2016 +
  pv.vote_g_2017 +
  pv.vote_g_2018) >= 2 then 1 
  else 0 end as general_at_least_twice_post08
  ,case 
  when (pv.vote_p_2008 +
  pv.vote_p_2012 +
  pv.vote_p_2013 +
  pv.vote_p_2014 +
  pv.vote_p_2015 +
  pv.vote_p_2016 +
  pv.vote_p_2017 +
  pv.vote_p_2018) >= 2 then 1 
  else 0 end as primary_at_least_twice_post08
  ,case 
  when pv.vote_g_2018 = 1 and pv.vote_g_2016 != 1 and pv.vote_g_2014 != 1 or pv.vote_g_2012 != 1 then 1 
  else 0 end as vote_midterm_2018_voter
  ,case 
  when pv.vote_g_2018 = 1 and pv.vote_g_2016 = 1 and pv.vote_g_2014 != 1 or pv.vote_g_2012 != 1 then 1 
  else 0 end as vote_pres_midterm_2018_2016_voter
  ,case  
  when pv.vote_g_2018 != 1 and pv.vote_g_2016 = 1 and pv.vote_g_2014 != 1 or pv.vote_g_2012 != 1 then 1 
  else 0 end as vote_pres_2016_voter
  ,case  
  when pv.vote_g_2018 != 1 and pv.vote_g_2016 != 1 and (pv.vote_g_2014 = 1 or pv.vote_g_2012 = 1 or pv.vote_g_2008 = 1) then 1 
  else 0 end as vote_lapsed_voter

from phoenix_analytics.person p 
left join phoenix_analytics.person_votes pv using(person_id)
left join phoenix_scores.all_scores_2020 as20 using(person_id) 
left join phoenix_scores.all_scores_2018 as18 using(person_id) 
left join phoenix_scores.all_scores_2016 as16 using(person_id) 
left join phoenix_consumer.tsmart_consumer tc using(person_id) 
left join bernie_nmarchio2.geo_county_covariates cty on left(p.census_block_group_2010,5) = lpad(cty.county_fip_id,5,'00000')
left join bernie_nmarchio2.geo_tract_covariates trc on left(p.census_block_group_2010,11) = lpad(trc.tract_id,11,'00000000000')
left join bernie_nmarchio2.geo_block_covariates bg on p.census_block_group_2010 = lpad(bg.block_group_id, 12,'000000000000') 
left join phoenix_census.acs_current acs on p.census_block_group_2010 = acs.block_group_id 
left join bernie_data_commons.master_xwalk_dnc x using(person_id)
left join l2.demographics l2 using(lalvoterid)

where p.is_deceased = false -- is alive
and p.reg_record_merged = false -- removes duplicated registration addresses
and p.reg_on_current_file = true --  voter was on the last voter file that the DNC processed and not eligible to vote in primaries
and p.reg_voter_flag = true -- voters who are registered to vote (i.e. have a registration status of active or inactive) even if they have moved states and their new state has not updated their file to reflect this yet
--and p.state_code in ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA')
);

grant select on table bernie_data_commons.rainbow_modeling_frame to group bernie_data;
