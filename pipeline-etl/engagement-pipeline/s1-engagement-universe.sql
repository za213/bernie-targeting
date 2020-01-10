
--USER TABLE
CREATE TEMP TABLE user_universe AS
     (SELECT * FROM (
              (SELECT DISTINCT id::varchar(256) AS user_id,
                               email::varchar(256),
                               first_name::varchar(256) AS user_firstname,
                               last_name::varchar(256) AS user_lastname,
                               address1::varchar(256) AS user_address_line_1,
                               address2::varchar(256) AS user_address_line_2,
                               city::varchar(256) AS user_city,
                               coalesce(STATE,region)::varchar(256) AS user_state,
                               coalesce(zip,postal)::varchar(256) AS user_zip,
                               NULL AS user_phone,
                               TO_Timestamp(updated_at,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date,
                               'actionkit' AS source_data,
                               ROW_NUMBER() OVER(PARTITION BY id ORDER BY email NULLS LAST, address1 NULLS LAST) AS rownum
               FROM ak_bernie.core_user WHERE id IN (SELECT distinct user_id FROM ak_bernie.events_eventsignup))
            UNION ALL
              (SELECT DISTINCT user_id::varchar(256),
                               coalesce(user__email_address)::varchar(256) AS email,
                               coalesce(user__given_name)::varchar(256) AS user_firstname,
                               coalesce(user__family_name)::varchar(256) AS user_lastname,
                               NULL AS user_address_line_1,
                               NULL AS user_address_line_2,
                               NULL AS user_city,
                               state_code::varchar(256) AS user_state,
                               user__postal_code::varchar(256) AS user_zip,
                               user__phone_number::varchar(256) AS user_phone,
                               TO_Timestamp(user__modified_date,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date,
                               'mobilize' AS source_data,
                               ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY email NULLS LAST, user__phone_number NULLS LAST) AS rownum
               FROM mobilize.participations_comprehensive)
           ) WHERE rownum = 1);

CREATE TEMP TABLE user_universe_2 AS
  (SELECT * FROM
     (SELECT coalesce(xwalk_master.person_id_master,xwalk_ak.person_id_ak,akmatch.person_id_actionkit,mobmatch.person_id_mobilize) AS person_id,
             user_universe.source_data,
             user_universe.user_id,
             coalesce(user_universe.email,xwalk_master.email_master) AS user_email,
             user_universe.user_firstname,
             user_universe.user_lastname,
             user_universe.user_address_line_1,
             user_universe.user_address_line_2,
             user_universe.user_city,
             user_universe.user_state,
             user_universe.user_zip,
             user_universe.user_phone,
             user_universe.user_modified_date
      FROM user_universe
      LEFT JOIN
        (SELECT person_id AS person_id_master,
                email AS email_master,
                ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY email NULLS LAST) AS rownum
         FROM bernie_data_commons.master_xwalk) xwalk_master ON (xwalk_master.email_master = user_universe.email AND xwalk_master.rownum = 1)
      LEFT JOIN
        (SELECT actionkit_id,
                person_id AS person_id_ak,
                ROW_NUMBER() OVER(PARTITION BY actionkit_id ORDER BY row_id DESC) AS rownum
         FROM bernie_data_commons.master_xwalk_ak) xwalk_ak ON (xwalk_ak.actionkit_id = user_universe.user_id AND xwalk_ak.rownum = 1)
      LEFT JOIN
        (SELECT person_id AS person_id_mobilize,
                user_id_mobilize
         FROM bernie_nmarchio2.events_users_xwalk WHERE user_id_mobilize IS NOT NULL AND person_id IS NOT NULL) mobmatch ON mobmatch.user_id_mobilize = user_universe.user_id AND user_universe.source_data = 'mobilize'
      LEFT JOIN
        (SELECT person_id AS person_id_actionkit,
                user_id_actionkit
         FROM bernie_nmarchio2.events_users_xwalk WHERE user_id_actionkit IS NOT NULL AND person_id IS NOT NULL) akmatch ON akmatch.user_id_actionkit = user_universe.user_id AND user_universe.source_data = 'actionkit' ));

DROP TABLE IF EXISTS bernie_nmarchio2.events_users;
CREATE TABLE bernie_nmarchio2.events_users DISTKEY (person_id) AS
  (SELECT *
   FROM
     (SELECT coalesce(user_universe_2.source_data,'0')||'_'||coalesce(user_universe_2.user_email,'0')|| '_' ||coalesce(user_universe_2.user_id,'0') as unique_id,
     	     user_universe_2.person_id, --first_value(user_universe_2.person_id) over(partition by user_universe_2.user_email order by user_universe_2.user_email NULLS LAST rows between unbounded preceding and unbounded following) as person_id,
             user_universe_2.source_data,
             user_universe_2.user_id,
             user_universe_2.user_email, 
             user_universe_2.user_firstname,
             user_universe_2.user_lastname,
             coalesce(user_universe_2.user_address_line_1,p.user_address_line_1) as user_address_line_1,
             coalesce(user_universe_2.user_address_line_2,p.user_address_line_2) as user_address_line_2,
             coalesce(user_universe_2.user_city,p.user_city) as user_city,
             coalesce(user_universe_2.user_state,p.user_state) as user_state,
             coalesce(user_universe_2.user_zip,p.user_zip) as user_zip,
             user_universe_2.user_phone, --first_value(user_universe_2.user_phone) over(partition by user_universe_2.user_email order by user_universe_2.user_email NULLS LAST rows between unbounded preceding and unbounded following) as user_phone,
             user_universe_2.user_modified_date,
             p.user_address_latitude,
             p.user_address_longitude
             FROM user_universe_2
      LEFT JOIN
        (SELECT person_id,
                voting_street_address AS user_address_line_1,
                voting_street_address_2 AS user_address_line_2,
                voting_city AS user_city,
                state_code AS user_state,
                voting_zip AS user_zip,
                voting_address_latitude AS user_address_latitude,
                voting_address_longitude AS user_address_longitude
         FROM phoenix_analytics.person) p using(person_id) ));

--SIGNUPS TABLE
DROP TABLE IF EXISTS bernie_nmarchio2.events_signups;
CREATE TABLE bernie_nmarchio2.events_signups AS
(SELECT * FROM 
  (SELECT 'mobilize' AS source_data ,
          mob.id::varchar(256) AS signup_id ,
          TO_Timestamp(mob.user__modified_date,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date ,
          mob.user_id::varchar(256) AS user_id ,
          xw.ak_event_id::varchar(256) ,
          mob.event_id::varchar(256) AS mobilize_id ,
          mob.timeslot_id::varchar(256) AS mobilize_timeslot_id ,
          mob.attended::boolean AS user_attended ,
          mob.status::varchar(256) AS status
   FROM
     (SELECT * FROM mobilize.participations_comprehensive) mob
   LEFT JOIN
     (SELECT DISTINCT ak_event_id,
             mobilize_id,
             mobilize_timeslot_id,
             row_number() over (partition BY mobilize_id || '_' || mobilize_timeslot_id ORDER BY ak_event_id NULLS LAST) AS dedupe
      FROM core_table_builds.events_xwalk) xw ON mob.event_id = xw.mobilize_id AND mob.timeslot_id = xw.mobilize_timeslot_id AND xw.dedupe = 1)
UNION ALL
  (SELECT 'actionkit' AS source_data ,
          ak.id::varchar(256) AS signup_id ,
          TO_Timestamp(ak.updated_at,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date ,
          ak.user_id::varchar(256) AS user_id ,
          ak.event_id::varchar(256) AS ak_event_id ,
          xw.mobilize_id::varchar(256) ,
          xw.mobilize_timeslot_id::varchar(256) ,
          ak.attended::boolean AS user_attended ,
          ak.status::varchar(256) AS status
   FROM
     (SELECT * FROM ak_bernie.events_eventsignup) ak
   LEFT JOIN
     (SELECT DISTINCT ak_event_id,
             mobilize_id,
             mobilize_timeslot_id,
             row_number() over (partition BY ak_event_id ORDER BY mobilize_id NULLS LAST, mobilize_timeslot_id NULLS LAST) AS dedupe
      FROM core_table_builds.events_xwalk) xw ON xw.ak_event_id = ak.event_id AND xw.dedupe = 1));

--EVENTS TABLE
DROP TABLE IF EXISTS bernie_nmarchio2.events_details;
CREATE TABLE bernie_nmarchio2.events_details AS
  (SELECT coalesce(ak_event_id,'0')||'_'||coalesce(mobilize_id,'0')||'_'||coalesce(mobilize_timeslot_id,'0')||'_'||coalesce(van_event_van_id,'0')||'_'||coalesce(van_timeslot_id,'0')||'_'||coalesce(event_campaign,'0')||'_'||coalesce(van_location_id,'0')||'_'||coalesce(mob_shift_count,'0')||'_'||coalesce(mob_shift_order,'0') as unique_id ,
  	      ak_event_id::varchar(256) ,
          mobilize_id::varchar(256) ,
          mobilize_timeslot_id::varchar(256) ,
          van_event_van_id::varchar(256) ,
          van_timeslot_id::varchar(256) ,
          coalesce(ak_title,event_title_ak,event_title_mob)::varchar(256) AS event_title ,
          coalesce(van_location_name,mobilize_venue,ak_venue) AS event_venue ,
          coalesce(mobilize_start_utc::timestamptz,ak_start_utc::timestamptz) AS event_start_utc ,
          coalesce(ak_address1,mobilize_address1,event_address_line_1_ak,event_address_line_1_mob) AS event_address1 ,
          coalesce(event_address_line_2_mob,event_address_line_2_ak)::varchar(256) AS event_address2 ,
          coalesce(event_city_mob,event_city_ak)::varchar(256) AS event_city ,
          case WHEN event_state IN ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') THEN event_state
          WHEN event_state_ak IN ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') THEN event_state_ak
          WHEN event_state_mob IN ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') THEN event_state_mob
          ELSE NULL END AS event_state ,
          coalesce(event_zip_mob,event_zip_ak)::varchar(256) AS event_zip ,
          coalesce(event_lon_ak,event_lon_mob)::varchar(256) AS event_lon ,
          coalesce(event_lat_ak,event_lat_mob)::varchar(256) AS event_lat ,
          coalesce(event_type,event_type_mob)::varchar(256) AS event_type_v1 ,
          CASE
          WHEN lower(coalesce(event_name,event_type_mob)) ILIKE '%barnstorm%' THEN 'barnstorm'
          WHEN lower(coalesce(event_name,event_type_mob)) SIMILAR TO ('%canvass%|%bernie-on-the-ballot%|%bernie-journey%|%signature_gathering%') THEN 'canvass'
          WHEN lower(coalesce(event_name,event_type_mob)) SIMILAR TO ('%solidarityevent%|%solidarity-event%|%solidarity_event%') THEN 'solidarity-action'
          WHEN lower(coalesce(event_name,event_type_mob)) SIMILAR TO ('%event-bernie-sanders%|%bernie-2020-event%') THEN 'rally-town-hall'
          WHEN lower(coalesce(event_name,event_type_mob)) SIMILAR TO ('%postcards_for_bernie%|%plan-win-party%|%debate-watch-party%|%debate_watch_party%|%office_opening%|%house-party%|%house_party%') THEN 'small-event'
          WHEN lower(coalesce(event_name,event_type_mob)) SIMILAR TO ('%call-bernie_virtual%|%phone_bank%|%phonebank%|%phone-bank%') THEN 'phonebank'
          WHEN lower(coalesce(event_name,event_type_mob)) SIMILAR TO ('%friend-to-friend%|%friend_to_friend%') THEN 'friend-to-friend'
          WHEN lower(coalesce(event_name,event_type_mob)) SIMILAR TO ('%volunteer_training%|%training%') THEN 'training'
          WHEN lower(coalesce(ak_title,event_title_ak,event_title_mob)) SIMILAR TO ('%rally%|%town hall%|%panel%|%first friday%|%community meeting%|%community conversation%|%brunch%|%alexandria%|%phillip agnew%|%aoc%|%bernie on the ballot%|%bernie breakfast%|%bernie 2020 discussion%|%nina turner%|%bernie 2020 meets%|%townhall%|%roundtable%') THEN 'rally-town-hall'
          WHEN lower(coalesce(ak_title,event_title_ak,event_title_mob)) SIMILAR TO ('%petition%|%canvass%|%petitions%|%volunteering%|%help get bernie%|%bernie journey%') THEN 'canvass'
          WHEN lower(coalesce(ak_title,event_title_ak,event_title_mob)) SIMILAR TO ('%phonebank%|%recruitment%|%make calls%') THEN 'phonebank'
          WHEN lower(coalesce(ak_title,event_title_ak,event_title_mob)) SIMILAR TO ('%barnstorm%') THEN 'barnstorm'
          WHEN lower(coalesce(ak_title,event_title_ak,event_title_mob)) SIMILAR TO ('%training%') THEN 'training'
          WHEN lower(coalesce(ak_title,event_title_ak,event_title_mob)) SIMILAR TO ('%parade%|%march%|%teachers for bernie%') THEN 'solidarity-action'
          WHEN lower(coalesce(ak_title,event_title_ak,event_title_mob)) SIMILAR TO ('%organizing meeting%') THEN 'solidarity-action'
          WHEN lower(coalesce(event_title_ak,event_title_mob,ak_title)) SIMILAR TO ('%delegates%|%meeting%|%talk bernie%|%community discussion%|%meet up%|%team meeting%|%student%|%high school%|% for bernie%|%kickoff meeting%|%leadership meeting%|%kick-off meeting%|%headquarters%|%bernie virtual meeting%|%bash%|%parranda%|%office opening%|%party%|%art hop%|%plan to win%|%debate watch party%|%dinner%|%postcards%|%potluck%|%volunteer appreciation%|%social hour%|%club meeting%|%unidos con bernie%|%debate%|%food drive%|%fiesta%|%happy hour%|%tabling%|%bonfire%') THEN 'small-event'
          ELSE 'other' END AS event_type_v2,
          event_name::varchar(256) ,
          event_campaign_mob::varchar(256) ,
          event_campaign::varchar(256) ,
          van_location_id::varchar(256) ,
          mob_shift_count::varchar(256) ,
          mob_shift_order::varchar(256)
   FROM (
           (SELECT ak_event_id ,
                   mobilize_id ,
                   event_type ,
                   mobilize_timeslot_id ,
                   van_event_van_id ,
                   van_timeslot_id ,
                   mob_shift_count ,
                   mob_shift_order ,
                   ak_title ,
                   CASE WHEN ak_address1 SIMILAR TO '%Exact location TBD%|%Address provided upon RSVP%' THEN NULL ELSE ak_address1 END AS ak_address1 ,
                   CASE WHEN ak_venue SIMILAR TO '%Unnamed venue%|%Private venue%' THEN NULL ELSE ak_venue END AS ak_venue ,
                   CASE WHEN mobilize_address1 SIMILAR TO '%Exact location TBD%|%Address provided upon RSVP%' THEN NULL ELSE mobilize_address1 END AS mobilize_address1 ,
                   CASE WHEN mobilize_venue SIMILAR TO '%Unnamed venue%|%Private venue%' THEN NULL ELSE mobilize_venue END AS mobilize_venue ,
                   ak_start_utc ,
                   mobilize_start_utc ,
                   van_location_id ,
                   CASE WHEN van_location_name SIMILAR TO '%Unnamed venue%|%Private venue%' THEN NULL ELSE van_location_name END AS van_location_name ,
                   event_state
            FROM core_table_builds.events_xwalk)
         LEFT JOIN
(SELECT id::varchar(256) AS mobilize_id,
        location__address_line_1::varchar(256) AS event_address_line_1_mob,
        location__address_line_2::varchar(256) AS event_address_line_2_mob,
        location__locality::varchar(256) AS event_city_mob,
        location__region::varchar(256) AS event_state_mob,
        location__postal_code::varchar(256) AS event_zip_mob,
        location__lat::real AS event_lat_mob,
        location__lon::real AS event_lon_mob,
        lower(event_type::varchar(256)) AS event_type_mob,
        lower(title::varchar(256)) AS event_title_mob,
        event_campaign__slug::varchar(256) AS event_campaign_mob
 FROM mobilize.events_comprehensive) using(mobilize_id)
         LEFT JOIN
(SELECT event.id::int AS ak_event_id,
        event.address1::varchar(256) AS event_address_line_1_ak,
        event.address2::varchar(256) AS event_address_line_2_ak,
        event.city::varchar(256) AS event_city_ak,
        coalesce(event.state,event.region)::varchar(256) AS event_state_ak,
        coalesce(event.zip,event.postal) AS event_zip_ak,
        event.longitude::real AS event_lon_ak,
        event.latitude::real AS event_lat_ak,
        event.title AS event_title_ak ,
        event.campaign_id AS event_campaign ,
        campaign.name AS event_name
 FROM
   (SELECT * FROM ak_bernie.events_event) event
 LEFT JOIN
   (SELECT id AS campaign_id, name FROM ak_bernie.events_campaign) campaign using(campaign_id)) using(ak_event_id)));

