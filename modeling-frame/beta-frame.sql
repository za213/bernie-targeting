--Data Dictionary https://docs.google.com/spreadsheets/d/183LBmZGNbWdjmGxuBY8GrbtXuoUMobcm5BO331vs5Og/edit?usp=sharing

set query_group to 'importers';

drop table if exists bernie_data_commons.rainbow_modeling_frame; 
create table bernie_data_commons.rainbow_modeling_frame
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
  
  --Block population age, race, education
  ,coalesce(pop_under_5_acs_13_17,90) as pop_under_5_acs_13_17
  ,coalesce(pop_5_17_acs_13_17,246) as pop_5_17_acs_13_17
  ,coalesce(pop_18_24_acs_13_17,142) as pop_18_24_acs_13_17
  ,coalesce(pop_25_44_acs_13_17,388) as pop_25_44_acs_13_17
  ,coalesce(pop_45_64_acs_13_17,384) as pop_45_64_acs_13_17
  ,coalesce(pop_65plus_acs_13_17,219) as pop_65plus_acs_13_17
  ,coalesce(hispanic_acs_13_17,272) as hispanic_acs_13_17
  ,coalesce(nh_white_alone_acs_13_17,895) as nh_white_alone_acs_13_17
  ,coalesce(nh_blk_alone_acs_13_17,179) as nh_blk_alone_acs_13_17
  ,coalesce(college_acs_13_17,306) as college_acs_13_17
  ,coalesce(tot_prns_in_hhd_acs_13_17,1435) as tot_prns_in_hhd_acs_13_17
  
  ,coalesce(pct_hispanic_acs_13_17,16.7012646296691) as pct_hispanic_acs_13_17
  ,coalesce(pct_nh_white_alone_acs_13_17,62.2094341001194) as pct_nh_white_alone_acs_13_17
  ,coalesce(pct_nh_blk_alone_acs_13_17,12.7993612183865) as pct_nh_blk_alone_acs_13_17
  ,coalesce(pct_nh_aian_alone_acs_13_17,0.707012756572289) as pct_nh_aian_alone_acs_13_17
  ,coalesce(pct_nh_asian_alone_acs_13_17,4.48027868413529) as pct_nh_asian_alone_acs_13_17
  ,coalesce(pct_nh_nhopi_alone_acs_13_17,0.138450060583508) as pct_nh_nhopi_alone_acs_13_17
  ,coalesce(pct_nh_sor_alone_acs_13_17,0.214007315401825) as pct_nh_sor_alone_acs_13_17
  ,coalesce(pct_pop_5yrs_over_acs_13_17,93.6101607845451) as pct_pop_5yrs_over_acs_13_17
  ,coalesce(pct_othr_lang_acs_13_17,20.1166995829495) as pct_othr_lang_acs_13_17
  ,coalesce(pct_pop_25yrs_over_acs_13_17,68.4902424247925) as pct_pop_25yrs_over_acs_13_17
  ,coalesce(pct_not_hs_grad_acs_13_17,13.4398143467192) as pct_not_hs_grad_acs_13_17
  ,coalesce(pct_college_acs_13_17,28.9708128627636) as pct_college_acs_13_17
  
  --Block income, poverty, wealth, health insurance (block group)
  ,coalesce(med_hhd_inc_bg_acs_13_17,60742) as med_hhd_inc_bg_acs_13_17
  ,coalesce(aggregate_hh_inc_acs_13_17,44004957) as aggregate_hh_inc_acs_13_17
  ,coalesce(med_house_value_bg_acs_13_17,228999) as med_house_value_bg_acs_13_17
  ,coalesce(med_house_value_tr_acs_13_17,238630) as med_house_value_tr_acs_13_17
  ,coalesce(aggr_house_value_acs_13_17,92372699) as aggr_house_value_acs_13_17
  ,coalesce(mail_return_rate_cen_2010,77.8642421162023) as mail_return_rate_cen_2010
  ,coalesce(pct_pov_univ_acs_13_17,97.456398162981) as pct_pov_univ_acs_13_17
  ,coalesce(pct_prs_blw_pov_lev_acs_13_17,15.8553983762712) as pct_prs_blw_pov_lev_acs_13_17
  ,coalesce(pct_no_health_ins_acs_13_17,10.2128929872888) as pct_no_health_ins_acs_13_17
  ,coalesce(pct_pub_asst_inc_acs_13_17,2.84260713297059) as pct_pub_asst_inc_acs_13_17
  ,coalesce(avg_agg_hh_inc_acs_13_17,77557) as avg_agg_hh_inc_acs_13_17
  ,coalesce(pct_no_plumb_acs_13_17,2.28981470976642) as pct_no_plumb_acs_13_17
  ,coalesce(avg_agg_house_value_acs_13_17,164679) as avg_agg_house_value_acs_13_17
    
  --Census contactibility measures
  ,coalesce(low_response_score,19.0304782693538) as low_response_score
  ,coalesce(pct_no_ph_srvc_acs_13_17,2.3958790054321) as pct_no_ph_srvc_acs_13_17
  ,coalesce(pct_hhd_nocompdevic_acs_13_17_tract,14.0227970658165) as pct_hhd_nocompdevic_acs_13_17_tract
  ,coalesce(pct_hhd_w_computer_acs_13_17_tract,75.1982375986167) as pct_hhd_w_computer_acs_13_17_tract
  ,coalesce(pct_hhd_w_onlysphne_acs_13_17_tract,4.29331000756511) as pct_hhd_w_onlysphne_acs_13_17_tract
  ,coalesce(pct_hhd_no_internet_acs_13_17_tract,19.0959819788177) as pct_hhd_no_internet_acs_13_17_tract
  ,coalesce(pct_hhd_w_broadband_acs_13_17_tract,63.788187749919) as pct_hhd_w_broadband_acs_13_17_tract
  ,coalesce(mail_return_rate_cen_2010_tract,77.3798754458014) as mail_return_rate_cen_2010_tract
  ,coalesce(low_response_score_tract,20.4549997298174) as low_response_score_tract
  ,coalesce(self_response_rate_acs_13_17_tract,59.2657827191181) as self_response_rate_acs_13_17_tract  
  ,coalesce(pct_pop_nocompdevic_acs_13_17_tract,9.82876877769372) as pct_pop_nocompdevic_acs_13_17_tract
  ,coalesce(pct_pop_w_broadcomp_acs_13_17_tract,79.380795687885) as pct_pop_w_broadcomp_acs_13_17_tract
  
  /*
  --Language spoken (block group)
  ,coalesce(pct_diff_hu_1yr_ago_acs_13_17,14.1286465145196) as pct_diff_hu_1yr_ago_acs_13_17
  ,coalesce(pct_eng_vw_span_acs_13_17,3.55591399410956) as pct_eng_vw_span_acs_13_17
  ,coalesce(pct_eng_vw_indoeuro_acs_13_17,0.640391501064182) as pct_eng_vw_indoeuro_acs_13_17
  ,coalesce(pct_eng_vw_api_acs_13_17,0.834475600956629) as pct_eng_vw_api_acs_13_17
  ,coalesce(pct_eng_vw_other_acs_13_17,0.179089840576882) as pct_eng_vw_other_acs_13_17
  ,coalesce(pct_eng_vw_acs_13_17,5.20987011985097) as pct_eng_vw_acs_13_17
  */
  
  --Household composition (block group)
  ,coalesce(pct_rel_family_hhd_acs_13_17,65.5190489070009) as pct_rel_family_hhd_acs_13_17
  ,coalesce(pct_mrdcple_hhd_acs_13_17,47.1347214746979) as pct_mrdcple_hhd_acs_13_17
  ,coalesce(pct_not_mrdcple_hhd_acs_13_17,52.1478290682847) as pct_not_mrdcple_hhd_acs_13_17
  ,coalesce(pct_female_no_hb_acs_13_17,13.3563738388161) as pct_female_no_hb_acs_13_17
  ,coalesce(pct_nonfamily_hhd_acs_13_17,33.7635020444098) as pct_nonfamily_hhd_acs_13_17
  ,coalesce(pct_sngl_prns_hhd_acs_13_17,27.4661560104739) as pct_sngl_prns_hhd_acs_13_17
  ,coalesce(pct_hhd_ppl_und_18_acs_13_17,31.2046174616645) as pct_hhd_ppl_und_18_acs_13_17
  ,coalesce(avg_tot_prns_in_hhd_acs_13_17,2.63179168349542) as avg_tot_prns_in_hhd_acs_13_17
  ,coalesce(pct_rel_under_6_acs_13_17,20.1481426957165) as pct_rel_under_6_acs_13_17
  ,coalesce(pct_hhd_moved_in_acs_13_17,39.9119242864987) as pct_hhd_moved_in_acs_13_17
  
  --Home and owner / occupancy (block group)
  ,coalesce(pct_tot_occp_units_acs_13_17,87.6827366954533) as pct_tot_occp_units_acs_13_17
  ,coalesce(pct_vacant_units_acs_13_17,11.6252209369342) as pct_vacant_units_acs_13_17
  ,coalesce(pct_renter_occp_hu_acs_13_17,35.4815095504114) as pct_renter_occp_hu_acs_13_17
  ,coalesce(pct_owner_occp_hu_acs_13_17,63.8010405841431) as pct_owner_occp_hu_acs_13_17
  ,coalesce(pct_single_unit_acs_13_17,69.259552771185) as pct_single_unit_acs_13_17
  ,coalesce(pct_mlt_u2_9_strc_acs_13_17,13.1689796103596) as pct_mlt_u2_9_strc_acs_13_17
  ,coalesce(pct_mlt_u10p_acs_13_17,10.869025082026) as pct_mlt_u10p_acs_13_17
  ,coalesce(pct_mobile_homes_acs_13_17,5.93147719382638) as pct_mobile_homes_acs_13_17
  ,coalesce(pct_crowd_occp_u_acs_13_17,3.4240591403949) as pct_crowd_occp_u_acs_13_17
  ,coalesce(pct_recent_built_hu_acs_13_17,0.663407062176377) as pct_recent_built_hu_acs_13_17

  --Tract socioeconomic battery
  ,coalesce(tot_population_acs_13_17_tract,4383) as tot_population_acs_13_17_tract
  ,coalesce(median_age_acs_13_17_tract,38.9029314816816) as median_age_acs_13_17_tract
  ,coalesce(civ_noninst_pop_acs_13_17_tract,4315) as civ_noninst_pop_acs_13_17_tract
  ,coalesce(tot_prns_in_hhd_acs_13_17_tract,4273) as tot_prns_in_hhd_acs_13_17_tract
  ,coalesce(med_hhd_inc_acs_13_17_tract,60312) as med_hhd_inc_acs_13_17_tract
  ,coalesce(aggregate_hh_inc_acs_13_17_tract,130999155) as aggregate_hh_inc_acs_13_17_tract
  ,coalesce(med_house_value_acs_13_17_tract,237591) as med_house_value_acs_13_17_tract
  ,coalesce(pct_college_acs_13_17_tract,29.2599319139738) as pct_college_acs_13_17_tract
  ,coalesce(pct_prs_blw_pov_lev_acs_13_17_tract,15.9299831135848) as pct_prs_blw_pov_lev_acs_13_17_tract
  ,coalesce(pct_no_health_ins_acs_13_17_tract,10.2100414730358) as pct_no_health_ins_acs_13_17_tract
  ,coalesce(pct_civ_unemp_16p_acs_13_17_tract,7.16511496271479) as pct_civ_unemp_16p_acs_13_17_tract
  ,coalesce(pct_pop_disabled_acs_13_17_tract,13.2864662812061) as pct_pop_disabled_acs_13_17_tract
  ,coalesce(pct_children_in_pov_acs_13_17_tract,20.8320197503512) as pct_children_in_pov_acs_13_17_tract
  ,coalesce(pct_nohealthins_u19_acs_13_17_tract,5.37268358910623) as pct_nohealthins_u19_acs_13_17_tract
  ,coalesce(pct_nohealthins1964_acs_13_17_tract,14.7285166972874) as pct_nohealthins1964_acs_13_17_tract
  ,coalesce(pct_nohealthins_65p_acs_13_17_tract,1.02902464065708) as pct_nohealthins_65p_acs_13_17_tract
  ,coalesce(pct_schl_enroll_3_4_acs_13_17_tract,47.3126352264131) as pct_schl_enroll_3_4_acs_13_17_tract
  ,coalesce(avg_tot_prns_in_hhd_acs_13_17_tract,2.61867083648546) as avg_tot_prns_in_hhd_acs_13_17_tract
  ,coalesce(pct_pub_asst_inc_acs_13_17_tract,2.80919255917) as pct_pub_asst_inc_acs_13_17_tract
  ,coalesce(avg_agg_hh_inc_acs_13_17_tract,77210) as avg_agg_hh_inc_acs_13_17_tract
  ,coalesce(avg_agg_house_value_acs_13_17_tract,170426) as avg_agg_house_value_acs_13_17_tract
  ,coalesce(pct_mrdcple_w_child_acs_13_17_tract,41.1085693829028) as pct_mrdcple_w_child_acs_13_17_tract

-- TargetSmart Scores
  -- Turnout 
  ,coalesce(ts_tsmart_partisan_score,53.615874715767) as ts_tsmart_partisan_score
  ,coalesce(ts_tsmart_presidential_general_turnout_score,72.1139112743012) as ts_tsmart_presidential_general_turnout_score
  ,coalesce(ts_tsmart_midterm_general_turnout_score,49.2156783912222) as ts_tsmart_midterm_general_turnout_score
  ,coalesce(ts_tsmart_offyear_general_turnout_score,15.2742025259369) as ts_tsmart_offyear_general_turnout_score
  ,coalesce(ts_tsmart_presidential_primary_turnout_score,37.3231147653052) as ts_tsmart_presidential_primary_turnout_score
  ,coalesce(ts_tsmart_non_presidential_primary_turnout_score,17.8722169888302) as ts_tsmart_non_presidential_primary_turnout_score
  ,coalesce(ts_tsmart_midterm_general_enthusiasm_score,40.6038213693376) as ts_tsmart_midterm_general_enthusiasm_score
  ,coalesce(ts_tsmart_local_voter_score,35.6934520560535) as ts_tsmart_local_voter_score
  
  -- Ideology and issue scores
  ,coalesce(ts_tsmart_tea_party_score,46.0826663961952) as ts_tsmart_tea_party_score
  ,coalesce(ts_tsmart_ideology_score,58.2238378150888) as ts_tsmart_ideology_score
  ,coalesce(ts_tsmart_moral_authority_score,40.7086519676708) as ts_tsmart_moral_authority_score
  ,coalesce(ts_tsmart_moral_care_score,48.4815620568045) as ts_tsmart_moral_care_score
  ,coalesce(ts_tsmart_moral_equality_score,52.5460436289233) as ts_tsmart_moral_equality_score
  ,coalesce(ts_tsmart_moral_equity_score,48.5739288914354) as ts_tsmart_moral_equity_score
  ,coalesce(ts_tsmart_moral_loyalty_score,49.7838475335626) as ts_tsmart_moral_loyalty_score
  ,coalesce(ts_tsmart_moral_purity_score,46.0161865880091) as ts_tsmart_moral_purity_score
  ,coalesce(ts_tsmart_college_graduate_score,42.7505476530988) as ts_tsmart_college_graduate_score
  ,coalesce(ts_tsmart_high_school_only_score,50.8139362209521) as ts_tsmart_high_school_only_score
  ,coalesce(ts_tsmart_prochoice_score,56.2537590536877) as ts_tsmart_prochoice_score
  ,coalesce(ts_tsmart_path_to_citizen_score,54.5802946861594) as ts_tsmart_path_to_citizen_score
  ,coalesce(ts_tsmart_climate_change_score,57.5431390157507) as ts_tsmart_climate_change_score
  ,coalesce(ts_tsmart_gun_control_score,54.7735550919433) as ts_tsmart_gun_control_score
  ,coalesce(ts_tsmart_gunowner_score,54.0492868926705) as ts_tsmart_gunowner_score
  ,coalesce(ts_tsmart_trump_resistance_score,39.8009590035495) as ts_tsmart_trump_resistance_score
  ,coalesce(ts_tsmart_trump_support_score,48.8257967230227) as ts_tsmart_trump_support_score
  ,coalesce(ts_tsmart_veteran_score,38.026382268614) as ts_tsmart_veteran_score
  ,coalesce(ts_tsmart_activist_score,42.7382851129463) as ts_tsmart_activist_score
  ,coalesce(ts_tsmart_working_class_score,58.6177878423767) as ts_tsmart_working_class_score
  
  -- Predictwise
  ,coalesce(predictwise_authoritarianism_score,71) as predictwise_authoritarianism_score
  ,coalesce(predictwise_compassion_score,55) as predictwise_compassion_score
  ,coalesce(predictwise_economic_populism_score,43) as predictwise_economic_populism_score
  ,coalesce(predictwise_free_trade_score,43) as predictwise_free_trade_score
  ,coalesce(predictwise_globalism_score,46) as predictwise_globalism_score
  ,coalesce(predictwise_guns_score,42) as predictwise_guns_score
  ,coalesce(predictwise_healthcare_women_score,53) as predictwise_healthcare_women_score
  ,coalesce(predictwise_healthcare_score,45) as predictwise_healthcare_score
  ,coalesce(predictwise_immigrants_score,41) as predictwise_immigrants_score
  ,coalesce(predictwise_military_score,53) as predictwise_military_score
  ,coalesce(predictwise_populism_score,38) as predictwise_populism_score
  ,coalesce(predictwise_poor_score,51) as predictwise_poor_score
  ,coalesce(predictwise_racial_resentment_score,66) as predictwise_racial_resentment_score
  ,coalesce(predictwise_regulation_score,59) as predictwise_regulation_score
  ,coalesce(predictwise_religious_freedom_score,49) as predictwise_religious_freedom_score
  ,coalesce(predictwise_taxes_score,44) as predictwise_taxes_score
  ,coalesce(predictwise_traditionalism_score,52) as predictwise_traditionalism_score
  ,coalesce(predictwise_trust_in_institutions_score,45) as predictwise_trust_in_institutions_score
  ,coalesce(predictwise_environmentalism_score,53) as predictwise_environmentalism_score
  ,coalesce(predictwise_presidential_score,55) as predictwise_presidential_score
  
  /*
  -- FEC contributions
  ,coalesce(enh_addr_size,40) as enh_addr_size
  ,coalesce(fec_zip_avg_contribution_amount,285.292351174354) as fec_zip_avg_contribution_amount
  ,coalesce(fec_zip_contribution_category_corporation,92676) as fec_zip_contribution_category_corporation
  ,coalesce(fec_zip_contribution_category_democrat,514867) as fec_zip_contribution_category_democrat
  ,coalesce(fec_zip_contribution_category_house,289501) as fec_zip_contribution_category_house
  ,coalesce(fec_zip_contribution_category_labor_organization,14300) as fec_zip_contribution_category_labor_organization
  ,coalesce(fec_zip_contribution_category_membership_organization,40105) as fec_zip_contribution_category_membership_organization
  ,coalesce(fec_zip_contribution_category_presidential,184526) as fec_zip_contribution_category_presidential
  ,coalesce(fec_zip_contribution_category_qualified_party,225967) as fec_zip_contribution_category_qualified_party
  ,coalesce(fec_zip_contribution_category_republican,400768) as fec_zip_contribution_category_republican
  ,coalesce(fec_zip_contribution_category_senate,223010) as fec_zip_contribution_category_senate
  ,coalesce(fec_zip_contribution_category_trade_association,49267) as fec_zip_contribution_category_trade_association
  ,coalesce(fec_zip_contribution_category_unaffiliated,487791) as fec_zip_contribution_category_unaffiliated
  ,coalesce(fec_zip_total_contribution_amount,1408448) as fec_zip_total_contribution_amount
  ,coalesce(fec_zip_total_contributions,3919) as fec_zip_total_contributions
  ,coalesce(gsyn_synth_hh_average_age,50.3527862005709) as gsyn_synth_hh_average_age
  ,coalesce(gsyn_synth_hh_pct_registered,95.974742397142) as gsyn_synth_hh_pct_registered
  ,coalesce(gsyn_synth_hh_total_primary_votes_person,1.62222639559819) as gsyn_synth_hh_total_primary_votes_person
  ,coalesce(gsyn_synth_hh_total_votes_per_person,5.47473457667572) as gsyn_synth_hh_total_votes_per_person
  ,coalesce(gsyn_synth_zip5_pct_catholic,14.4101302276016) as gsyn_synth_zip5_pct_catholic
  ,coalesce(gsyn_synth_zip5_pct_dem_primary_votes,0.233256642021729) as gsyn_synth_zip5_pct_dem_primary_votes
  ,coalesce(gsyn_synth_zip5_pct_jewish,1.10422757474685) as gsyn_synth_zip5_pct_jewish
  ,coalesce(gsyn_synth_zip5_pct_of_dem_fec_contributions,0.529875692892941) as gsyn_synth_zip5_pct_of_dem_fec_contributions
  ,coalesce(gsyn_synth_zip5_pct_of_dems_per_reg_count,0.494239755293577) as gsyn_synth_zip5_pct_of_dems_per_reg_count
  ,coalesce(gsyn_synth_zip5_sum_dem_primary_votes_cast_count,7515) as gsyn_synth_zip5_sum_dem_primary_votes_cast_count
  ,coalesce(gsyn_synth_zip5_sum_fec_contribution_count_democrat,85) as gsyn_synth_zip5_sum_fec_contribution_count_democrat
  ,coalesce(gsyn_synth_zip5_sum_fec_contribution_count_republican,51) as gsyn_synth_zip5_sum_fec_contribution_count_republican
  ,coalesce(gsyn_synth_zip5_sum_individuals_in_zip5,25065) as gsyn_synth_zip5_sum_individuals_in_zip5
  ,coalesce(gsyn_synth_zip5_sum_primary_votes_cast_count,33423) as gsyn_synth_zip5_sum_primary_votes_cast_count
  ,coalesce(gsyn_synth_zip5_sum_registered,19430) as gsyn_synth_zip5_sum_registered
  ,coalesce(gsyn_synth_zip5_sum_unregistered,5666) as gsyn_synth_zip5_sum_unregistered
  ,coalesce(gsyn_synth_zip5_sum_zip5_democrat,10504) as gsyn_synth_zip5_sum_zip5_democrat
  ,coalesce(gsyn_synth_zip5_sum_zip5_republican,7051) as gsyn_synth_zip5_sum_zip5_republican
  ,coalesce(gsyn_synth_zip5_total_fec_contributions,141) as gsyn_synth_zip5_total_fec_contributions
  ,coalesce(gsyn_synth_zip9_pct_dem_primary_votes,0.24470400461151) as gsyn_synth_zip9_pct_dem_primary_votes
  */
  
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
  ,vote_g_2018
  ,vote_g_2016
  ,vote_g_2014
  ,vote_g_2012
  ,case
    when vote_g_2018 = 1 and vote_g_2016 != 1
    and vote_g_2014 != 1 or vote_g_2012 != 1 
    then 1 else 0 end as vote_midterm_2018_voter
  ,case  
    when vote_g_2018 = 1 and vote_g_2016 = 1
    and vote_g_2014 != 1 or vote_g_2012 != 1  
    then 1 else 0 end as vote_pres_midterm_2018_2016_voter
  ,case  
    when vote_g_2018 != 1 and vote_g_2016 = 1
    and vote_g_2014 != 1 or vote_g_2012 != 1  
    then 1 else 0 end as vote_pres_2016_voter
  ,case  
    when vote_g_2018 != 1 and vote_g_2016 != 1
    and (vote_g_2014 = 1 or vote_g_2012 = 1)  
    then 1 else 0 end as vote_lapsed_2014_2012_voter
  ,case  
    when registration_date::date > '2018-11-08' then 1 else 0 end as vote_new_reg
  
from phoenix_analytics.person p 
left join phoenix_analytics.person_votes pv using(person_id)
left join phoenix_scores.all_scores_2020 score using(person_id) 
left join phoenix_scores.all_scores_2018 score using(person_id) 
left join phoenix_scores.all_scores_2016 score using(person_id) 
left join phoenix_consumer.tsmart_consumer tc using(person_id) 
left join bernie_nmarchio2.census_pdb_blocktract on gidtr = census_tract_2010
left join bernie_nmarchio2.census_pdb_block on gidbg = census_block_group_2010
left join bernie_data_commons.master_xwalk_dnc x using(person_id)
left join l2.demographics l2 using(lalvoterid)
left join phoenix_census.acs_current on block_group_id = p.census_block_group_2010
left join bernie_nmarchio2.primaryreturns16 pri on p.county_fips = right(census_county_fips,'3') and p.state_fips = left(lpad(census_county_fips,5,'000'),2)

  where p.is_deceased = false -- is alive
  and p.reg_record_merged = false -- removes duplicated registration addresses
  and p.reg_on_current_file = true --  voter was on the last voter file that the DNC processed and not eligible to vote in primaries
  and p.reg_voter_flag = true -- voters who are registered to vote (i.e. have a registration status of active or inactive) even if they have moved states and their new state has not updated their file to reflect this yet

);

grant select on table bernie_data_commons.rainbow_modeling_frame to group bernie_data;
