
-- https://github.com/Bernie-2020/universe_review/blob/master/GOTV%20Universes/gotv_person_flags.sql
-- https://github.com/Bernie-2020/data-analytics/blob/master/ad_hoc/person_primary_votes.sql

begin;

set wlm_query_slot_count to 4;

DROP TABLE IF EXISTS bernie_nmarchio2.base_universe;
CREATE TABLE bernie_nmarchio2.base_universe
distkey(person_id) 
sortkey(person_id) AS
  (SELECT p.person_id::varchar,
          p.state_code,
          p.voting_address_id,
          p.county_fips,
          p.county_name,
          p.dnc_precinct_id,
          p.van_precinct_id,
          p.voting_address_latitude,
          p.voting_address_longitude,

          voteinfo.party_affiliation,
          voteinfo.civis_2020_partisanship,
          voteinfo.vote_history,
          voteinfo.registered_in_state,
          voteinfo.registration_action,
          voteinfo.early_vote_mode,

          rainbo.party_3way,
          rainbo.race_5way,
          rainbo.spanish_language_2way,
          rainbo.age_5way,
          rainbo.ideology_5way,
          rainbo.education_2way,
          rainbo.income_5way,
          rainbo.gender_2way,
          rainbo.urban_3way,
          rainbo.child_in_hh_2way,
          rainbo.marital_2way,
          rainbo.religion_9way,
          bdcas.donut_segment,

          -- Support scores
          bdcas.current_support_raw,
          NTILE(100) OVER (PARTITION BY p.state_code ORDER BY bdcas.current_support_raw ASC) AS current_support_raw_100,
          bdcas.field_id_1_score,
          NTILE(100) OVER (PARTITION BY p.state_code ORDER BY bdcas.field_id_1_score ASC) AS field_id_1_score_100,
          bdcas.field_id_composite_score,
          NTILE(100) OVER (PARTITION BY p.state_code ORDER BY bdcas.field_id_composite_score ASC) AS field_id_composite_score_100,

          -- Supplementary scores
          bdcas.sanders_very_excited_score,
          NTILE(10) OVER (PARTITION BY p.state_code ORDER BY bdcas.sanders_very_excited_score ASC) AS sanders_very_excited_score_10,
          bdcas.turnout_current,
          round((bdcas.current_support_raw * bdcas.turnout_current * voteinfo.civis_2020_partisanship),4) AS bernie_net_votes_current,
          NTILE(10) OVER (PARTITION BY p.state_code ORDER BY bernie_net_votes_current ASC) AS bernie_net_votes_current_10,
          bdcas.sanders_strong_support_score,
          NTILE(10) OVER (PARTITION BY p.state_code ORDER BY bdcas.sanders_strong_support_score ASC) AS sanders_strong_support_score_10,
          bdcas.biden_support,
          NTILE(10) OVER (PARTITION BY p.state_code ORDER BY bdcas.biden_support ASC) AS biden_support_10,
          bdcas.warren_support,
          NTILE(10) OVER (PARTITION BY p.state_code ORDER BY bdcas.warren_support ASC) AS warren_support_10,
          bdcas.buttigieg_support,
          NTILE(10) OVER (PARTITION BY p.state_code ORDER BY bdcas.buttigieg_support ASC) AS buttigieg_support_10,

          -- Message targeting
          voteinfo.civis_2018_one_pct_persuasion,
          voteinfo.civis_2018_marijuana_persuasion,
          voteinfo.civis_2018_college_persuasion,
          voteinfo.civis_2018_welcome_persuasion,
          voteinfo.civis_2018_sexual_assault_persuasion,
          voteinfo.civis_2018_economic_persuasion,

          CASE WHEN ccj.contact_made > 0 THEN 1 ELSE 0 END AS contact_made ,
          CASE WHEN ccj.negative_result > 0 THEN 1 ELSE 0 END AS negative_contact ,
          CASE WHEN ccj.id_1 > 0 THEN 1 ELSE 0 END AS id1 ,
          CASE WHEN ccj.id_2 > 0 THEN 1 ELSE 0 END AS id2 ,
          CASE WHEN ccj.id_3 > 0 THEN 1 ELSE 0 END AS id3 ,
          CASE WHEN ccj.id_4 > 0 THEN 1 ELSE 0 END AS id4 ,
          CASE WHEN ccj.id_5 > 0 THEN 1 ELSE 0 END AS id5 ,
          CASE WHEN id1_household = 1 AND ccj.id_1 <> 1 THEN 1 ELSE 0 END AS id1_household ,

          CASE WHEN p_household.donor_household = 1 THEN 1 ELSE 0 END AS donors_and_housemates ,
          CASE WHEN p_household.event_household = 1 THEN 1 ELSE 0 END AS event_attendee_or_rsvp_and_housemates ,
          CASE WHEN p_household.volunteer_household = 1 THEN 1 ELSE 0 END AS volunteers_and_housemates ,

          CASE
          --WHEN person_flags = 1 THEN 1 
              --WHEN ccj.negative_contact = 1 THEN 0
              WHEN vol.action_taken = 1 
              OR donors_and_housemates = 1 
              OR event_attendee_or_rsvp_and_housemates = 1 
              OR volunteers_and_housemates = 1 THEN 1
              ELSE 0 END AS activism_flags
        
     FROM 

-- PERSON
      (SELECT person_id::varchar,
              state_code,
              voting_address_id,
              county_fips,
              county_name,
              dnc_precinct_id,
              van_precinct_id,
              voting_address_latitude,
              voting_address_longitude
      FROM phoenix_analytics.person
      WHERE is_deceased = FALSE
        AND reg_record_merged = FALSE
        AND reg_on_current_file = TRUE
        AND reg_voter_flag = TRUE 
        AND state_code IN ('AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA')) p

-- IDs
    LEFT JOIN
    (SELECT * FROM (SELECT person_id, actionkit_id, jsonid, row_number() OVER (PARTITION BY person_id ORDER BY actionkit_id NULLS LAST) AS rn FROM bernie_data_commons.master_xwalk_dnc) WHERE rn = 1) xwalk
    using(person_id)

-- SCORES
      LEFT JOIN
     (SELECT person_id::varchar,
             donut_segment,
             coalesce(NULLIF(current_support_raw/100,0),.01) AS current_support_raw,
             field_id_1_score/100 AS field_id_1_score,
             field_id_composite_score/100  AS field_id_composite_score,
             coalesce(NULLIF(turnout_current/100,0),.01) AS turnout_current,
             sanders_strong_support_score/100 AS sanders_strong_support_score,
             sanders_very_excited_score/100 AS sanders_very_excited_score,
             biden_support/100 AS biden_support,
             warren_support/100 AS warren_support,
             buttigieg_support/100 AS buttigieg_support
      FROM bernie_data_commons.all_scores) bdcas
     using(person_id)
      --WHERE state_code IN ('NC','TN'))
   --LEFT JOIN
     --(SELECT DISTINCT person_id::varchar,
        --CASE 
           --WHEN f_attended_2_events = 1
             --OR f_rsvpd_3_or_more_events = 1
             --OR f_donor_in_household = 1
             --OR f_event_attendee_in_household = 1
             --OR f_rsvpd_2_events = 1
             ----OR f_id_1_other_party = 1
             --OR f_hosted_1_event = 1
             ----OR f_id_1_last_60_days = 1
             ----OR f_id_1_npp = 1
             ----OR f_id_1_dem = 1
             --OR f_ctc_dem = 1
             --OR f_ctc_last_60_days = 1
             --OR f_ctc_npp = 1
             --OR f_donated = 1
             --OR f_core_donut_top50 = 1 THEN 1
             --ELSE 0 END AS person_flags -- refine logic / this is placeholder
      --FROM gotv_universes.gotv_person_flags
      --WHERE person_id IS NOT NULL AND person_flags = 1) using(person_id)

   --LEFT JOIN
    --(SELECT person_id::varchar, support_int from bernie_data_commons.ccj_dnc where unique_id_flag = 1 and support_int IN  (1,2,3,4,5) ) ccjdnc 
     --using(person_id)

-- DEMO INFO
   LEFT JOIN bernie_data_commons.rainbow_analytics_frame rainbo 
   using(person_id)

-- CONTACT INFO
   LEFT JOIN 
   (SELECT person_id::varchar , 
           CASE WHEN resultcode IN ('Canvassed', 'Do Not Contact', 'Refused', 'Call Back', 'Language Barrier', 'Hostile', 'Come Back', 'Cultivation', 'Refused Contact', 'Spanish', 'Other', 'Not Interested') THEN 1 ELSE 0 END AS contact_made , 
           CASE WHEN resultcode IN ('Do Not Contact','Hostile','Refused','Refused Contact') OR (support_int = 4 OR support_int = 5) THEN 1 ELSE 0 END AS negative_result ,
           COUNT(DISTINCT CASE WHEN support_int = 1 AND unique_id_flag=TRUE THEN person_id END) AS id_1 ,
           COUNT(DISTINCT CASE WHEN support_int = 2 AND unique_id_flag=TRUE THEN person_id END) AS id_2 ,
           COUNT(DISTINCT CASE WHEN support_int = 3 AND unique_id_flag=TRUE THEN person_id END) AS id_3 ,
           COUNT(DISTINCT CASE WHEN support_int = 4 AND unique_id_flag=TRUE THEN person_id END) AS id_4 ,
           COUNT(DISTINCT CASE WHEN support_int = 5 AND unique_id_flag=TRUE THEN person_id END) AS id_5
           FROM bernie_data_commons.ccj_dnc WHERE person_id IS NOT NULL GROUP BY 1,2,3) ccj
   using(person_id)

-- VOLUNTEER INFO
    LEFT JOIN
    (SELECT DISTINCT 
        person_id::varchar, 
        1 AS action_taken 
        FROM ( (SELECT person_id, 1 AS action_taken FROM
              (SELECT person_id,
                      sum(teacher_myc+student_myc+labor_myc+attended_canvass+canvass_myc+mvp_myc+slack_vol+attended_phonebank+attended_training+volunteer_shifts_myc+activist_myc+attended_solidarity_action+attended_friend_to_friend+attended_small_event+commit2caucus_id+sticker_id+attended_other+bern_attempted_voter_lookup+signups_solidarity_action+signups_training+signups_small_event+signups_friend_to_friend+volunteer_yes_id+volunteer_yes_maybe_id+signups_canvass+signups_phonebank+volunteer_signup_myc+bern_universe+constituencies_myc+other_activist_myc+bern_is_student+student_myc+teacher_myc+bern_is_union+labor_myc+attended_rally_town_hall+attended_barnstorm+events_myc) AS actions_count 
                      FROM bernie_nmarchio2.universe_engagement GROUP BY 1) WHERE action_taken > 0)
            UNION ALL
           (SELECT person_id, 1 AS action_taken FROM (SELECT DISTINCT person_id FROM bernie_jshuman.donor_basetable WHERE n_donations > 0 OR n_clicks > 5)))) vol
    using(person_id)

-- HOUSEHOLD INFO
    LEFT JOIN 
    (SELECT 
        voting_address_id ,
        max(CASE WHEN ccj.person_id IS NOT NULL THEN 1 ELSE 0 END) AS id1_household ,
        max(CASE WHEN donors.user_id IS NOT NULL THEN 1 ELSE 0 END) AS donor_household ,
        max(CASE WHEN events.person_id IS NOT NULL AND (events.events_attended >= 1 OR events.events_rsvpd >= 1) THEN 1 ELSE 0 END) AS event_household ,
        max(CASE WHEN events.person_id IS NOT NULL AND events.volunteer >= 1 THEN 1 ELSE 0 END) AS volunteer_household
    FROM
        (SELECT person_id, voting_address_id FROM phoenix_analytics.person WHERE is_deceased = FALSE AND reg_voter_flag = TRUE) p
    JOIN bernie_data_commons.master_xwalk_dnc xw ON xw.person_id = p.person_id
    LEFT JOIN bernie_data_commons.ccj_dnc ccj ON ccj.person_id = p.person_id AND ccj.unique_id_flag = 1 AND ccj.support_int = 1
    LEFT JOIN
        (SELECT co.user_id AS user_id
        FROM ak_bernie.core_order co
        LEFT JOIN ak_bernie.core_transaction ct ON co.id = ct.order_id
        WHERE ct.success = 1 
                AND coalesce(ct.created_at, co.created_at, NULL) >= date('2019-02-18') 
                AND ct.type IN ('sale','credit') 
                AND ct.status IN ('completed','') 
                AND ct.amount > 0 
                AND co.status = 'completed'
        GROUP BY 1) donors ON donors.user_id = xw.actionkit_id
    LEFT JOIN
        (SELECT xw.person_id ,
                xw.actionkit_id ,
                xw.st_myc_van_id ,
                xw.jsonid_encoded ,
                pasr.state,
                count(CASE WHEN status_name IN ('Completed', 'Walk in') THEN pasr.event_id END) AS events_attended ,
                count(pasr.event_id) AS events_rsvpd ,
                COUNT(DISTINCT CASE WHEN pasr.role <> 'Attendee' THEN pasr.myc_van_id END) AS volunteer
        FROM ptg.ptg_all_shifts_regioned pasr
        JOIN bernie_data_commons.master_xwalk_st_myc xw ON xw.myc_van_id = pasr.myc_van_id AND xw.state_code = pasr.state
        WHERE status_rank = 1 AND event_date::date < (CURRENT_DATE)::date
        GROUP BY 1,2,3,4,5) events ON events.person_id = xw.person_id GROUP BY 1) p_household 
        ON p_household.voting_address_id = p.voting_address_id

-- VOTER ELIGIBILITY INFO
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
           WHEN p.party_name_dnc = 'Democratic' OR p.party_id = 1 THEN 'Democratic' 
           WHEN p.party_name_dnc = 'Republican' OR p.party_id = 2 THEN 'Republican' 
           WHEN p.party_name_dnc = 'Other' OR p.party_id = 3 THEN 'Other' 
           WHEN p.party_name_dnc = 'Libertarian' OR p.party_id = 4 THEN 'Libertarian' 
           WHEN p.party_name_dnc = 'Green' OR p.party_id = 5 THEN 'Green' 
           WHEN p.party_name_dnc = 'Independent' OR p.party_id = 6 THEN 'Independent' 
           WHEN p.party_name_dnc = 'Nonpartisan' OR p.party_id = 7 THEN 'Nonpartisan' 
           WHEN p.party_name_dnc = 'Unaffiliated' OR p.party_id = 8 THEN 'Unaffiliated' 
           WHEN as20.civis_2020_partisanship >= .66 THEN 'Democratic' 
           WHEN as20.civis_2020_partisanship between .66 and .33 THEN 'Nonpartisan' 
           WHEN as20.civis_2020_partisanship <= .33 THEN 'Republican' 
           --('AL','GA','HI','IL','IN','MI','MN','MO','MS','MT','ND','OH','SC','TN','TX','VA','VT','WA','WI') 
           ELSE 'Other' END AS party_affiliation , -- verify logic
      CASE 
           WHEN p.state_code IN ('AL','AR','GA','IL','IN','MI','MO','MS','MT','MN','ND','OH','SC','TN','TX','UT','VA','VT','WI','WA') THEN '1 - Dem Primary Eligible' --open
           WHEN p.state_code IN ('OK','NE') AND party_affiliation IN ('Democratic','Independent') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code IN ('SD','RI') AND party_affiliation IN ('Democratic','Independent','Nonpartisan','Unaffiliated') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code = 'CA' AND party_affiliation IN ('Democratic','Nonpartisan','Unaffiliated') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code IN ('CO','ID','MA','NC','NH') AND party_affiliation IN ('Democratic', 'Unaffiliated','Nonpartisan') THEN '1 - Dem Primary Eligible' -- mixed
           WHEN p.state_code IN ('AK','AZ','CT','DC','DE','FL','HI','IA','KS','KY','LA','MD','ME','NJ','NM','NV','NY','OR','PA','WV','WY') AND party_affiliation = 'Democratic' THEN '1 - Dem Primary Eligible' -- mixed
           ELSE '2 - Must Register as Dem' END AS registration_action , -- verify logic
      CASE 
           WHEN ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2008_method_early = 1) AND pev.voted_early_by_mail_2008 = 1)
             OR ((pv.vote_g_2010_method_early = 1 OR pv.vote_p_2010_method_early = 1) AND pev.voted_early_by_mail_2010 = 1)
             OR ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2012_method_early = 1) AND pev.voted_early_by_mail_2012 = 1)
             OR ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2014_method_early = 1) AND pev.voted_early_by_mail_2014 = 1)
             OR ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2016_method_early = 1) AND pev.voted_early_by_mail_2016 = 1)
             OR (pev.voted_2019 = 1 AND pev.voted_early_by_mail_2019 = 1) THEN '1 - EV By Mail' 
           WHEN ((pv.vote_g_2008_method_early = 1 OR pv.vote_p_2008_method_early = 1) AND pev.voted_early_in_person_2008 = 1)
             OR ((pv.vote_g_2010_method_early = 1 OR pv.vote_p_2010_method_early = 1) AND pev.voted_early_in_person_2010 = 1)
             OR ((pv.vote_g_2012_method_early = 1 OR pv.vote_p_2012_method_early = 1) AND pev.voted_early_in_person_2012 = 1)
             OR ((pv.vote_g_2014_method_early = 1 OR pv.vote_p_2014_method_early = 1) AND pev.voted_early_in_person_2014 = 1)
             OR ((pv.vote_g_2016_method_early = 1 OR pv.vote_p_2016_method_early = 1) AND pev.voted_early_in_person_2016 = 1)
             OR (pev.voted_2019 = 1 AND pev.voted_early_in_person_2019 = 1) THEN '2 - EV In Person' 
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
             OR pv.vote_pp_2008_method_early = 1) THEN '3 - EV Mail/In Person' 
             ELSE '4 - Non-Early Voter' END AS early_vote_mode , -- verify logic
      CASE 
           WHEN p.is_permanent_absentee = 't' THEN '3 - Absentee voter'
           WHEN poos.to_state_code <> pncoa.state_code 
             AND (pv.vote_g_2018_method_absentee = 1 
               OR pv.vote_p_2018_method_absentee = 1) THEN '3 - Absentee voter'
           WHEN absent.requested_ballot = 1 THEN '3 - Absentee voter'
           WHEN p.ncoa_since_reg_date = 't' and p.out_of_state_ncoa = 't' THEN '3 - Absentee voter'
           WHEN (poos.to_state_code <> pncoa.state_code) THEN '2 - Registered in different state'
           ELSE '1 - Registered in current state' END AS registered_in_state, -- verify logic

          --as18.civis_2018_turnout::FLOAT8 as civis_2018_turnout,
          as20.civis_2020_partisanship::FLOAT8,
          as18.civis_2018_economic_persuasion::FLOAT8,
          as18.civis_2018_one_pct_persuasion::FLOAT8,
          as18.civis_2018_marijuana_persuasion::FLOAT8,
          as18.civis_2018_college_persuasion::FLOAT8,
          as18.civis_2018_welcome_persuasion::FLOAT8,
          as18.civis_2018_sexual_assault_persuasion::FLOAT8
          --(tc.ts_tsmart_presidential_primary_turnout_score::FLOAT8/100) as ts_tsmart_presidential_primary_turnout_score

      FROM phoenix_analytics.person p
      LEFT JOIN phoenix_scores.all_scores_2020 as20 using(person_id)
      LEFT JOIN phoenix_scores.all_scores_2018 as18 using(person_id)
      LEFT JOIN phoenix_analytics.person_ncoas_current pncoa using(person_id)
      --LEFT JOIN phoenix_consumer.tsmart_consumer tc using(person_id)
      LEFT JOIN phoenix_analytics.person_votes pv using(person_id)
      LEFT JOIN phoenix_analytics.person_out_of_state poos using(person_id)
      LEFT JOIN phoenix_analytics.person_early_votes pev using(person_id)
      LEFT JOIN bernie_data_commons.person_primary_votes ppv using(person_id)
      LEFT JOIN (SELECT person_id::varchar, 1 AS requested_ballot FROM (SELECT (state_code||'-'||myv_van_id) AS st_myv_van_id, election_id FROM phoenix_demssanders20_vansync.contacts_absentees WHERE date_request_received::date > '2019-11-08') 
        LEFT JOIN bernie_data_commons.master_xwalk_st_myv using(st_myv_van_id)) absent using(person_id)
      ) voteinfo using(person_id)
    
  );

commit;

begin;

set wlm_query_slot_count to 5;

DROP TABLE IF EXISTS bernie_nmarchio2.march_universe;
CREATE TABLE bernie_nmarchio2.march_universe 
distkey(person_id) 
sortkey(person_id) AS
(SELECT *,
    -- Rank order each gotv_segment
    row_number() OVER (PARTITION BY state_code ORDER BY bern_flags DESC, gotv_segment ASC, purpose_of_contact ASC, current_support_raw DESC) as gotv_rank 

    FROM
  (SELECT person_id,
          state_code,
          gotv_segment,
          bern_flags,
          current_support_raw,
          current_support_raw_100,
          sanders_very_excited_score,
          sanders_very_excited_score_100,
          field_id_1_score,
          field_id_1_score_100,
          purpose_of_contact,
          voter_readiness,
          registration_action,
          registered_in_state,
          early_vote_mode,
          vote_history,
          party_affiliation,
          dem_primary_electorate,
          turnout_current,
          -- Decile each gotv_segment
          NTILE(10) OVER (PARTITION BY state_code||'_'||gotv_segment ORDER BY current_support_raw ASC) AS current_support_raw_10_by_state_gotv_segment
          --civis_2018_turnout,
          --ts_tsmart_presidential_primary_turnout_score
   FROM
(select *,
        CASE WHEN (bern_flags = 1 
                OR registration_action = '1 - Dem Primary Eligible' 
                OR party_affiliation = 'Democratic' 
                OR current_support_raw_100 >= 70) THEN '1 - Target universe' -- Find flags to remove the hostile people from GOTV universe
            ELSE '2 - Non-target' END AS dem_primary_electorate, -- This defines the upper bound of the GOTV universe

        CASE
            WHEN dem_primary_electorate = '2 - Non-target' 
                THEN '6 - Non-target'
            WHEN (vote_history = '1 - Newly registered' OR vote_history = '2 - Dem Primary or 2018 voter')
                AND registration_action = '1 - Dem Primary Eligible' 
                AND registered_in_state = '1 - Registered in current state'  
                    THEN '1 - Vote-ready, active voter'
            WHEN registered_in_state = '1 - Registered in current state'
                AND registration_action = '1 - Dem Primary Eligible'
                AND vote_history IN ('3 - Lapsed voter','4 - No vote but eligible','5 - Other') 
                    THEN '2 - Vote-ready, less active voter'
            WHEN registration_action = '2 - Must Register as Dem'
                AND registered_in_state = '1 - Registered in current state' 
                    THEN '3 - Must register as Dem'
            WHEN registration_action = '1 - Dem Primary Eligible'
                AND registered_in_state IN ('2 - Registered in different state','3 - Likely absentee voter') 
                    THEN '4 - Must mail absentee or register in current state'
            WHEN registered_in_state IN ('2 - Registered in different state','3 - Likely absentee voter')
                AND registration_action = '2 - Must Register as Dem' 
                    THEN '5 - Must register as Dem, mail absentee or register in current state'
            ELSE '6 - Non-target' END AS voter_readiness, -- This adds flags for barriers that may hinder primary participation

        CASE 
            WHEN dem_primary_electorate = '1 - Target universe'
                AND voter_readiness IN ('1 - Vote-ready, active voter', '2 - Vote-ready, less active voter')
                AND (current_support_raw_100 >= 80 OR sanders_very_excited_score_100 >= 80 OR field_id_1_score_100 >= 80 OR bern_flags = 1) 
                   THEN '1 - Vote-ready GOTV target'
            WHEN dem_primary_electorate = '1 - Target universe'
                AND voter_readiness IN ( '3 - Must register as Dem', '4 - Must mail absentee or register in current state',  '5 - Must register as Dem, mail absentee or register in current state')
                AND (current_support_raw_100 >= 80 OR sanders_very_excited_score_100 >= 80 OR field_id_1_score_100 >= 80 OR bern_flags = 1)
                   THEN '2 - Registration-action GOTV target'
            WHEN dem_primary_electorate = '1 - Target universe'
                AND voter_readiness IN ('1 - Vote-ready, active voter', '2 - Vote-ready, less active voter')
                   THEN '3 - Vote-ready persuasion target'
            WHEN dem_primary_electorate = '1 - Target universe'
                AND voter_readiness NOT IN ('1 - Vote-ready, active voter', '2 - Vote-ready, less active voter')
                   THEN '4 - Registration-action and persuasion'
            ELSE '5 - Non-target' END AS purpose_of_contact, -- This breaks out persuasion from GOTV contact universes

        CASE 
            WHEN bern_flags = 1 THEN '1 - Verified Supporter'
            WHEN purpose_of_contact IN ('1 - Vote-ready GOTV target','2 - Registration-action GOTV target') THEN '2 - GOTV target'
            WHEN purpose_of_contact IN ('3 - Vote-ready persuasion target','4 - Registration-action and persuasion') THEN '3 - Persuasion target'
            ELSE '4 - Non-target' END AS gotv_segment -- This breaks out supporters, top GOTV targets, persuasion voters, and non-targets

FROM bernie_nmarchio2.base_universe)));

commit;


