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


