-- Coalesced address table
DROP TABLE IF EXISTS bernie_nmarchio2.events_users_enhanced;
CREATE TABLE bernie_nmarchio2.events_users_enhanced AS
(SELECT base_ids.unique_id ,
       coalesce(base_ids.person_id, new_ids.person_id) as person_id,
       base_ids.source_data ,
       base_ids.user_id ,
       base_ids.user_email ,
       base_ids.user_firstname ,
       base_ids.user_lastname ,
       base_ids.user_address_line_1 ,
       base_ids.user_address_line_2 ,
       base_ids.user_city ,
       base_ids.user_state ,
       base_ids.user_zip ,
       base_ids.user_phone ,
       base_ids.user_modified_date ,
       base_ids.user_address_latitude ,
       base_ids.user_address_longitude
FROM bernie_nmarchio2.events_users base_ids
LEFT JOIN
  (SELECT source_id,
          person_id,
          score
   FROM
     (SELECT source_id,
             matched_id,
             score
      FROM bernie_nmarchio2.events_users_match_output WHERE score >= 0.5) MATCH
   LEFT JOIN
     (SELECT person_id,
             voterbase_id,
             ROW_NUMBER() OVER(PARTITION BY voterbase_id ORDER BY person_id NULLS LAST) AS rownum
      FROM bernie_data_commons.master_xwalk) xwalk ON MATCH.matched_id = xwalk.voterbase_id AND xwalk.rownum = 1) new_ids 
  ON base_ids.unique_id = new_ids.source_id);

CREATE TEMP TABLE person_id_xwalk AS
(SELECT coalesce(person_id_mobilize,person_id_actionkit) AS person_id,
        user_id_mobilize,
        user_id_actionkit
FROM
  (SELECT DISTINCT person_id AS person_id_mobilize,
                   user_id AS user_id_mobilize
   FROM bernie_nmarchio2.events_users_enhanced WHERE source_data = 'mobilize' AND person_id IS NOT NULL)
FULL JOIN
  (SELECT DISTINCT person_id AS person_id_actionkit,
                   user_id AS user_id_actionkit
   FROM bernie_nmarchio2.events_users_enhanced WHERE source_data = 'actionkit' AND person_id IS NOT NULL) ON person_id_mobilize = person_id_actionkit);


CREATE TEMP TABLE email_xwalk AS
(SELECT coalesce(user_email_mobilize,user_email_actionkit) AS user_email,
       user_id_mobilize,
       user_id_actionkit
FROM
  (SELECT DISTINCT user_email AS user_email_mobilize,
                   user_id AS user_id_mobilize
   FROM bernie_nmarchio2.events_users_enhanced WHERE source_data = 'mobilize' AND person_id IS NULL AND user_email IS NOT NULL)
FULL JOIN
  (SELECT DISTINCT user_email AS user_email_actionkit,
                   user_id AS user_id_actionkit
   FROM bernie_nmarchio2.events_users_enhanced WHERE source_data = 'actionkit' AND person_id IS NULL AND user_email IS NOT NULL) ON user_email_mobilize = user_email_actionkit);

-- Mobilize-ActionKit User Crosswalk (some people have duplicative Mobilize and ActionKit IDs)
DROP TABLE IF EXISTS bernie_nmarchio2.events_users_xwalk;
CREATE TABLE bernie_nmarchio2.events_users_xwalk AS
(SELECT main.person_id,
	    coalesce(main.user_email,side.user_email_residual) AS user_email,
        main.user_id_mobilize,
        main.user_id_actionkit
  FROM ((SELECT NULL AS person_id,
                user_id_mobilize,
                user_id_actionkit,
                user_email
         FROM email_xwalk)
      UNION ALL
        (SELECT person_id,
                user_id_mobilize,
                user_id_actionkit,
                NULL AS user_email
         FROM person_id_xwalk)) main
   LEFT JOIN
    (SELECT DISTINCT person_id,
                     user_email AS user_email_residual,
                     ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY user_email NULLS LAST) AS rownum
     FROM bernie_nmarchio2.events_users_enhanced) side ON main.person_id = side.person_id AND side.rownum = 1);


--SIGNUPS TABLE
DROP TABLE IF EXISTS bernie_nmarchio2.events_signups;
CREATE TABLE bernie_nmarchio2.events_signups AS
(SELECT 
          coalesce(m.source_data, a.source_data) as source_data ,
          m.signup_id_mobilize ,
          a.signup_id_ak ,
          coalesce(m.user_modified_date_mobilize, a.user_modified_date_ak) as user_modified_date,
          coalesce(m.user_id, a.user_id) AS user_id ,
          coalesce(m.ak_event_id,a.ak_event_id) as ak_event_id ,
          coalesce(m.mobilize_id, a.mobilize_id) AS mobilize_id ,
          coalesce(m.mobilize_timeslot_id, a.mobilize_timeslot_id) AS mobilize_timeslot_id,
          coalesce(m.user_attended, a.user_attended) AS user_attended ,
          coalesce(m.status, a.status) AS status , 
          coalesce(m.person_id, a.person_id) AS person_id,
          coalesce(m.user_email, a.user_email) as user_email,
          coalesce(m.user_id_mobilize, a.user_id_mobilize) as user_id_mobilize,
          coalesce(m.user_id_actionkit, a.user_id_actionkit) as user_id_actionkit
 FROM
  (SELECT 'mobilize' AS source_data ,
          mob.id::varchar(256) AS signup_id_mobilize ,
          TO_Timestamp(mob.user__modified_date,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date_mobilize ,
          mob.user_id::varchar(256) AS user_id ,
          xw.ak_event_id::varchar(256) ,
          mob.event_id::varchar(256) AS mobilize_id ,
          mob.timeslot_id::varchar(256) AS mobilize_timeslot_id ,
          mob.attended::boolean AS user_attended ,
          mob.status::varchar(256) AS status , 
          xw_user.person_id,
          xw_user.user_email,
          xw_user.user_id_mobilize,
          xw_user.user_id_actionkit
   FROM
     (SELECT * FROM mobilize.participations_comprehensive) mob
   LEFT JOIN
     (SELECT DISTINCT ak_event_id,
             mobilize_id,
             mobilize_timeslot_id,
             row_number() over (partition BY mobilize_id || '_' || mobilize_timeslot_id ORDER BY ak_event_id NULLS LAST) AS dedupe
      FROM core_table_builds.events_xwalk) xw ON mob.event_id = xw.mobilize_id AND mob.timeslot_id = xw.mobilize_timeslot_id AND xw.dedupe = 1
   LEFT JOIN 
     (SELECT DISTINCT person_id,
                      user_email,
                      user_id_mobilize,
                      user_id_actionkit
      FROM bernie_nmarchio2.events_users_xwalk
      WHERE user_id_mobilize IS NOT NULL) xw_user ON mob.user_id = xw_user.user_id_mobilize) m
FULL JOIN
  (SELECT 'actionkit' AS source_data ,
          ak.id::varchar(256) AS signup_id_ak ,
          TO_Timestamp(ak.updated_at,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date_ak ,
          ak.user_id::varchar(256) AS user_id ,
          ak.event_id::varchar(256) AS ak_event_id ,
          xw.mobilize_id::varchar(256) ,
          xw.mobilize_timeslot_id::varchar(256) ,
          ak.attended::boolean AS user_attended ,
          ak.status::varchar(256) AS status ,
          xw_user.person_id,
          xw_user.user_email,
          xw_user.user_id_mobilize,
          xw_user.user_id_actionkit
   FROM
     (SELECT * FROM ak_bernie.events_eventsignup) ak
   LEFT JOIN
     (SELECT DISTINCT ak_event_id,
             mobilize_id,
             mobilize_timeslot_id,
             row_number() over (partition BY ak_event_id ORDER BY mobilize_id NULLS LAST, mobilize_timeslot_id NULLS LAST) AS dedupe
      FROM core_table_builds.events_xwalk) xw ON xw.ak_event_id = ak.event_id AND xw.dedupe = 1
   LEFT JOIN 
     (SELECT DISTINCT person_id,
                      user_email,
                      user_id_mobilize,
                      user_id_actionkit
      FROM bernie_nmarchio2.events_users_xwalk
      WHERE user_id_actionkit IS NOT NULL) xw_user ON ak.user_id = xw_user.user_id_actionkit) a
  ON (m.ak_event_id = a.ak_event_id AND m.user_id_actionkit = a.user_id_actionkit)  
     AND (m.mobilize_id = a.mobilize_id AND m.mobilize_timeslot_id = a.mobilize_timeslot_id AND m.user_id_mobilize = a.user_id_mobilize));

