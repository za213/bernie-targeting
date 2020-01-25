
-- https://github.com/Bernie-2020/universe_review/blob/master/GOTV%20Universes/gotv_person_flags.sql
-- https://github.com/Bernie-2020/data-analytics/blob/master/ad_hoc/person_primary_votes.sql

DROP TABLE IF EXISTS bernie_nmarchio2.march_universe;
CREATE TABLE bernie_nmarchio2.march_universe 
distkey(person_id) 
sortkey(person_id) AS
(SELECT *,
    -- Rank order each segment
    row_number() OVER (PARTITION BY state_code ORDER BY bern_flags DESC, gotv_segment ASC, gotv_segment_10 DESC, vote_history ASC, current_support_raw DESC) as gotv_rank -- placeholder swap vote_history with the contact_purpose bucket that orders groups according to chronology of GOTV contact actions
    FROM
  (SELECT person_id,
          state_code,
          gotv_segment,
          -- Decile each segment
          NTILE(10) OVER (PARTITION BY state_code||'_'||gotv_segment ORDER BY current_support_raw ASC) AS gotv_segment_10,
          bern_flags,
          current_support_raw,
          current_support_raw_100,
          sanders_very_excited_score,
          sanders_very_excited_score_100,
          sanders_strong_support_score,
          sanders_strong_support_score_100,
          field_id_1_score,
          field_id_1_score_100,
          vote_history,
          party_affiliation,
          registration_action,
          early_vote_mode,
          registered_in_state,
          dem_primary_electorate,
          turnout_current,
          civis_2018_turnout,
          ts_tsmart_presidential_primary_turnout_score
   FROM
  (SELECT person_id::varchar,
          state_code,
          current_support_raw,
          NTILE(100) OVER (PARTITION BY state_code ORDER BY current_support_raw ASC) AS current_support_raw_100,
          sanders_very_excited_score,
          NTILE(100) OVER (PARTITION BY state_code ORDER BY sanders_very_excited_score ASC) AS sanders_very_excited_score_100,
          sanders_strong_support_score,
          NTILE(100) OVER (PARTITION BY state_code ORDER BY sanders_strong_support_score ASC) AS sanders_strong_support_score_100,
          field_id_1_score,
          NTILE(100) OVER (PARTITION BY state_code ORDER BY field_id_1_score ASC) AS field_id_1_score_100,
          vote_history,
          party_affiliation,
          registration_action,
          early_vote_mode,
          registered_in_state,
          coalesce(bern_flags,0) AS bern_flags,
          CASE WHEN (bern_flags = 1 
                   OR registration_action = '1 - Dem Primary Eligible' 
                   OR party_affiliation = 'Democratic' 
                   OR current_support_raw_100 > 70) THEN '1 - Potential GOTV target' -- find flags to remove the hostile people from GOTV universe
               ELSE '2 - Non-target' END AS dem_primary_electorate,
          CASE WHEN bern_flags = 1 then '1 - Verified Supporter'
               WHEN (vote_history = '1 - Newly registered' OR vote_history = '2 - Dem Primary or 2018 voter')
                   AND registration_action = '1 - Dem Primary Eligible' 
                   AND registered_in_state = '1 - Registered in current state'
                   AND (current_support_raw_100 >= 80 AND sanders_very_excited_score_100 >= 80) THEN '2 - Vote-ready Supporter'
               WHEN dem_primary_electorate = '1 - Potential GOTV target' THEN '3 - Motivate Target'
               ELSE '4 - Non-target' END AS gotv_segment, -- refine segmenting / this is placeholder
          turnout_current,
          civis_2018_turnout,
          ts_tsmart_presidential_primary_turnout_score
   FROM
     (SELECT person_id::varchar,
             state_code,
             coalesce(current_support_raw,0) AS current_support_raw,
             coalesce(sanders_very_excited_score,0) AS sanders_very_excited_score,
             coalesce(sanders_strong_support_score,0) AS sanders_strong_support_score,
             coalesce(field_id_1_score,0) AS field_id_1_score,
             coalesce(turnout_current,0) AS turnout_current
      FROM bernie_data_commons.all_scores
      WHERE state_code IN ('SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VA','VT','PR','ID','MI','MS','MO','ND','WA','WY','AZ','FL','IL','OH','GA','ND'))
   LEFT JOIN
     (SELECT person_id::varchar,
        CASE 
           WHEN f_attended_2_events = 1
             OR f_rsvpd_3_or_more_events = 1
             OR f_donor_in_household = 1
             OR f_event_attendee_in_household = 1
             OR f_rsvpd_2_events = 1
             OR f_id_1_other_party = 1
             OR f_hosted_1_event = 1
             OR f_id_1_last_60_days = 1
             OR f_id_1_npp = 1
             OR f_id_1_dem = 1
             OR f_ctc_dem = 1
             OR f_ctc_last_60_days = 1
             OR f_ctc_npp = 1
             OR f_donated = 1
             OR f_core_donut_top50 = 1 THEN 1 
             ELSE 0 END AS bern_flags -- refine logic / this is placeholder
      FROM gotv_universes.gotv_person_flags
      WHERE person_id IS NOT NULL AND bern_flags = 1) using(person_id)
   LEFT JOIN
     (SELECT person_id::varchar,
        CASE 
           WHEN p.registration_date::date > '2018-11-08' THEN '1 - Newly registered' 
           WHEN (pv.vote_p_2018_party_d = 1
             OR pv.vote_p_2017_party_d = 1 
             OR pv.vote_p_2016_party_d = 1 
             OR pv.vote_p_2015_party_d = 1 
             OR pv.vote_p_2014_party_d = 1 
             OR pv.vote_p_2013_party_d = 1 
             OR pv.vote_p_2012_party_d = 1 
             OR pv.vote_p_2011_party_d = 1 
             OR pv.vote_p_2010_party_d = 1 
             OR pv.vote_p_2009_party_d = 1 
             OR pv.vote_p_2008_party_d = 1 
             OR pv.vote_p_2007_party_d = 1 
             OR pv.vote_p_2006_party_d = 1 
             OR pv.vote_p_2005_party_d = 1 
             OR pv.vote_p_2004_party_d = 1 
             OR pv.vote_p_2003_party_d = 1 
             OR pv.vote_p_2002_party_d = 1 
             OR pv.vote_p_2001_party_d = 1 
             OR pv.vote_p_2000_party_d = 1 
             OR pv.vote_p_1998_party_d = 1 
             OR pv.vote_p_1996_party_d = 1 
             OR pv.vote_pp_2016_party_d = 1 
             OR pv.vote_pp_2012_party_d = 1 
             OR pv.vote_pp_2008_party_d = 1 
             OR pv.vote_pp_2004_party_d = 1 
             OR pv.vote_pp_2000_party_d = 1 
             OR pv.vote_pp_1996_party_d = 1 
             OR ppv.voted_16pp_flag = 1 
             OR ppv.voted_08pp_flag = 1 
             OR ppv.voted_04pp_flag = 1) THEN '2 - Dem Primary or 2018 voter'
           WHEN (pv.vote_g_2018 = 1) THEN '2 - Dem Primary or 2018 voter'
           WHEN (pv.vote_g_2016 = 1
             OR pv.vote_g_2014 = 1
             OR pv.vote_g_2012 = 1
             OR pv.vote_g_2010 = 1
             OR pv.vote_g_2008 = 1
             OR pv.vote_g_2006 = 1
             OR pv.vote_g_2004 = 1
             OR pv.vote_g_2002 = 1
             OR pv.vote_g_2000 = 1
             OR pv.vote_g_1998 = 1) THEN '3 - Lapsed voter' 
           WHEN (pv.vote_g_2008_novote_eligible = 1
             OR pv.vote_g_2010_novote_eligible = 1
             OR pv.vote_g_2012_novote_eligible = 1
             OR pv.vote_g_2014_novote_eligible = 1
             OR pv.vote_g_2016_novote_eligible = 1
             OR pv.vote_g_2018_novote_eligible = 1) THEN '4 - No vote but eligible' 
             ELSE '5 - Other' END AS vote_history , 
       CASE 
           WHEN p.state_code IN ('AL','GA','HI','IL','IN','MI','MN','MO','MS','MT','ND','OH','SC','TN','TX','VA','VT','WA','WI') AND as20.civis_2020_partisanship >= .75 THEN 'Democratic' 
           WHEN p.state_code IN ('AL','GA','HI','IL','IN','MI','MN','MO','MS','MT','ND','OH','SC','TN','TX','VA','VT','WA','WI') AND (as20.civis_2020_partisanship >= .7 AND as20.civis_2020_partisanship < .75) THEN 'Green' 
           WHEN p.state_code IN ('AL','GA','HI','IL','IN','MI','MN','MO','MS','MT','ND','OH','SC','TN','TX','VA','VT','WA','WI') AND (as20.civis_2020_partisanship >= .6 AND as20.civis_2020_partisanship < .7) THEN 'Independent' 
           WHEN p.state_code IN ('AL','GA','HI','IL','IN','MI','MN','MO','MS','MT','ND','OH','SC','TN','TX','VA','VT','WA','WI') AND (as20.civis_2020_partisanship >= .5 AND as20.civis_2020_partisanship < .6) THEN 'Libertarian' 
           WHEN p.state_code IN ('AL','GA','HI','IL','IN','MI','MN','MO','MS','MT','ND','OH','SC','TN','TX','VA','VT','WA','WI') AND as20.civis_2020_partisanship < .5 THEN 'Republican' 
           WHEN p.party_name_dnc = 'Democratic' OR p.party_id = 1 THEN 'Democratic' 
           WHEN p.party_name_dnc = 'Republican' OR p.party_id = 2 THEN 'Republican' 
           WHEN p.party_name_dnc = 'Other' OR p.party_id = 3 THEN 'Other' 
           WHEN p.party_name_dnc = 'Libertarian' OR p.party_id = 4 THEN 'Libertarian' 
           WHEN p.party_name_dnc = 'Green' OR p.party_id = 5 THEN 'Green' 
           WHEN p.party_name_dnc = 'Independent' OR p.party_id = 6 THEN 'Independent' 
           WHEN p.party_name_dnc = 'Nonpartisan' OR p.party_id = 7 THEN 'Nonpartisan' 
           WHEN p.party_name_dnc = 'Unaffiliated' OR p.party_id = 8 THEN 'Unaffiliated' 
           ELSE 'Other' END AS party_affiliation , -- verify logic
      CASE 
           WHEN p.state_code IN ('AL','AR','GA','IL','IN','MI','MO','MS','MT','MN','ND','OH','SC','TN','TX','UT','VA','VT','WI','WA') THEN '1 - Dem Primary Eligible' --open
           WHEN p.state_code IN ('OK','NE') AND (party_affiliation = 'Democratic' OR party_affiliation = 'Independent') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code IN ('SD','RI') AND (party_affiliation = 'Democratic' OR party_affiliation = 'Unaffiliated' OR party_affiliation = 'Independent') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code = 'CA' AND (party_affiliation = 'Democratic' OR party_affiliation = 'Nonpartisan') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code IN ('CO','ID','MA','NC','NH') AND (party_affiliation = 'Democratic' OR party_affiliation = 'Unaffiliated') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code IN ('AK','AZ','CT','DC','DE','FL','HI','IA','KS','KY','LA','MD','ME','NJ','NM','NV','NY','OR','PA','WV','WY') AND (party_affiliation = 'Democratic') THEN '1 - Dem Primary Eligible' -- mixed
           ELSE '2 - Must Register as Dem' END AS registration_action , -- verify logic
      CASE 
           WHEN ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2008_method_early = 1) AND pev.voted_early_by_mail_2008 = 1)
             OR ((pv.vote_g_2010_method_early = 1 OR pv.vote_p_2010_method_early = 1) AND pev.voted_early_by_mail_2010 = 1)
             OR ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2012_method_early = 1) AND pev.voted_early_by_mail_2012 = 1)
             OR ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2014_method_early = 1) AND pev.voted_early_by_mail_2014 = 1)
             OR ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2016_method_early = 1) AND pev.voted_early_by_mail_2016 = 1)
             OR (pev.voted_2019 = 1 AND pev.voted_early_by_mail_2019 = 1) THEN 'EV By Mail' 
           WHEN ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2008_method_early = 1) AND pev.voted_early_in_person_2008 = 1)
             OR ((pv.vote_g_2010_method_early = 1 OR pv.vote_p_2010_method_early = 1) AND pev.voted_early_in_person_2010 = 1)
             OR ((pv.vote_g_2012_method_early = 1 OR pv.vote_p_2012_method_early = 1) AND pev.voted_early_in_person_2012 = 1)
             OR ((pv.vote_g_2014_method_early = 1 OR pv.vote_p_2014_method_early = 1) AND pev.voted_early_in_person_2014 = 1)
             OR ((pv.vote_g_2016_method_early = 1 OR pv.vote_p_2016_method_early = 1) AND pev.voted_early_in_person_2016 = 1)
             OR (pev.voted_2019 = 1 AND pev.voted_early_in_person_2019 = 1) THEN 'EV In Person' 
           WHEN (pv.vote_g_2018_method_early = 1
             OR pv.vote_g_2017_method_early = 1
             OR pv.vote_g_2016_method_early = 1
             OR pv.vote_g_2015_method_early = 1
             OR pv.vote_g_2014_method_early = 1
             OR pv.vote_g_2013_method_early = 1
             OR pv.vote_g_2012_method_early = 1
             OR pv.vote_g_2011_method_early = 1
             OR pv.vote_g_2010_method_early = 1
             OR pv.vote_g_2009_method_early = 1
             OR pv.vote_g_2008_method_early = 1
             OR pv.vote_p_2018_method_early = 1
             OR pv.vote_p_2017_method_early = 1
             OR pv.vote_p_2016_method_early = 1
             OR pv.vote_p_2015_method_early = 1
             OR pv.vote_p_2014_method_early = 1
             OR pv.vote_p_2013_method_early = 1
             OR pv.vote_p_2012_method_early = 1
             OR pv.vote_p_2011_method_early = 1
             OR pv.vote_p_2010_method_early = 1
             OR pv.vote_p_2009_method_early = 1
             OR pv.vote_p_2008_method_early = 1
             OR pv.vote_pp_2016_method_early = 1
             OR pv.vote_pp_2012_method_early = 1
             OR pv.vote_pp_2008_method_early = 1) THEN 'EV Mail/In Person' 
             ELSE 'Non-Early Voter' END AS early_vote_mode , -- verify logic
      CASE 
           WHEN poos.to_state_code <> pncoa.state_code 
             AND (pv.vote_g_2018_method_absentee = 1 
                OR pv.vote_p_2018_method_absentee = 1 
                OR pv.vote_g_2017_method_absentee = 1 
                OR pv.vote_g_2016_method_absentee = 1
                OR pv.vote_p_2016_method_absentee = 1) THEN '3 - Likely absentee voter'
           WHEN (poos.to_state_code <> pncoa.state_code) THEN '2 - Registered in different state'
           ELSE '1 - Registered in current state' END AS registered_in_state, -- verify logic

          as18.civis_2018_turnout,
          tc.ts_tsmart_presidential_primary_turnout_score

      FROM phoenix_analytics.person p
      LEFT JOIN phoenix_scores.all_scores_2020 as20 using(person_id)
      LEFT JOIN phoenix_scores.all_scores_2018 as18 using(person_id)
      LEFT JOIN phoenix_analytics.person_ncoas_current pncoa using(person_id)
      LEFT JOIN phoenix_consumer.tsmart_consumer tc using(person_id)
      LEFT JOIN phoenix_analytics.person_votes pv using(person_id)
      LEFT JOIN phoenix_analytics.person_out_of_state poos using(person_id)
      LEFT JOIN phoenix_analytics.person_early_votes pev using(person_id)
      LEFT JOIN bernie_data_commons.person_primary_votes ppv using(person_id) 
      WHERE person_id IS NOT NULL) using(person_id)) 
));







