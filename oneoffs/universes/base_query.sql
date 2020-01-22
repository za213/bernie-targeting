
-- https://github.com/Bernie-2020/universe_review/blob/master/GOTV%20Universes/gotv_person_flags.sql

drop table if exists bernie_data_commons.march_universe; 
create table bernie_data_commons.march_universe
distkey(person_id)
sortkey(person_id)
as 
(SELECT person_id,
       state_code,
       row_number() OVER (PARTITION BY state_code ORDER BY bern_flags DESC, current_support_raw DESC, turnout_priority ASC) as rank_order,
       bern_flags,
       current_support_raw,
       turnout_priority,
       CASE WHEN bern_flags = 1 THEN 100 ELSE current_support_raw_100 END AS support_targets_100,
       CASE WHEN bern_flags = 1 THEN 20 ELSE current_support_raw_20 END AS support_targets_20,
       CASE WHEN bern_flags = 1 THEN 10 ELSE current_support_raw_10 END AS support_targets_10
FROM
(SELECT person_id::varchar,
    state_code,
       current_support_raw,
       coalesce(bern_flags,0) AS bern_flags,
       NTILE(100) OVER (PARTITION BY state_code||'_'||coalesce(bern_flags,0) ORDER BY current_support_raw ASC) AS current_support_raw_100,
       NTILE(20) OVER (PARTITION BY state_code||'_'||coalesce(bern_flags,0)ORDER BY current_support_raw ASC) AS current_support_raw_20,
       NTILE(10) OVER (PARTITION BY state_code||'_'||coalesce(bern_flags,0) ORDER BY current_support_raw ASC) AS current_support_raw_10,
       turnout_priority
       FROM
(SELECT person_id::varchar,
    state_code,
    coalesce(current_support_raw,0) AS current_support_raw
    FROM bernie_data_commons.all_scores 
    WHERE state_code IN ('AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VA','VT','PR','ID','MI','MS','MO','ND','WA','WY','AZ','FL','IL','OH','GA','ND'))
LEFT JOIN
  (SELECT person_id::varchar,
          CASE
              WHEN actionkit_id IS NOT NULL
                   OR st_myc_van_id IS NOT NULL
                   OR student_hash IS NOT NULL
                   OR field_support_int = 1
                   OR f_ctc_dem = 1
                   OR f_ctc_npp = 1
                   OR f_ctc_other_party = 1
                   OR field_support_int = 1
                   OR f_donor_in_household = 1
                   OR f_event_rsvp_in_household = 1
                   OR f_event_attendee_in_household = 1
                   OR f_in_student_roster_reg = 1
                   OR f_in_student_roster_unreg = 1 THEN 1 ELSE 0
          END AS bern_flags
   FROM gotv_universes.gotv_person_flags WHERE person_id IS NOT NULL AND bern_flags = 1) 
  using(person_id)
  LEFT JOIN
 (SELECT person_id::varchar,
       CASE
           WHEN p.registration_date::date > '2018-11-08' THEN '1 - Primary voter/newly registered'
           WHEN (pv.vote_p_2008_party_d = 1
                 OR pv.vote_p_2009_party_d = 1
                 OR pv.vote_p_2010_party_d = 1
                 OR pv.vote_p_2011_party_d = 1
                 OR pv.vote_p_2012_party_d = 1
                 OR pv.vote_p_2013_party_d = 1
                 OR pv.vote_p_2014_party_d = 1
                 OR pv.vote_p_2015_party_d = 1
                 OR pv.vote_p_2016_party_d = 1
                 OR pv.vote_p_2017_party_d = 1
                 OR pv.vote_p_2018_party_d = 1
                 OR pv.vote_pp_2000_party_d = 1
                 OR pv.vote_pp_2004_party_d = 1
                 OR pv.vote_pp_2008_party_d = 1
                 OR pv.vote_pp_2016_party_d = 1
                 OR ppv.voted_16pp_flag = 1
                 OR ppv.voted_08pp_flag = 1
                 OR ppv.voted_04pp_flag = 1) THEN '1 - Primary voter/newly registered'
           WHEN (pv.vote_g_2018 = 1
                 OR pv.vote_g_2016 = 1
                 OR pv.vote_g_2014 = 1
                 OR pv.vote_g_2012 = 1
                 OR pv.vote_g_2010 = 1
                 OR pv.vote_g_2008 = 1) THEN '2 - General only voter'
           WHEN (pv.vote_g_2008_novote_eligible = 1
                 OR pv.vote_g_2010_novote_eligible = 1
                 OR pv.vote_g_2012_novote_eligible = 1
                 OR pv.vote_g_2014_novote_eligible = 1
                 OR pv.vote_g_2016_novote_eligible = 1
                 OR pv.vote_g_2018_novote_eligible = 1) THEN '3 - No vote but eligible'
           ELSE '4 - Other' END AS turnout_priority
FROM phoenix_analytics.person p
LEFT JOIN phoenix_analytics.person_votes pv using(person_id)
LEFT JOIN bernie_data_commons.person_primary_votes ppv using(person_id) WHERE person_id is not null) using(person_id)));
