

DROP TABLE IF EXISTS bernie_nmarchio2.events_ak_mobilize;

CREATE TABLE bernie_nmarchio2.events_ak_mobilize DISTKEY (person_id) AS
  (SELECT *
   FROM (
           (SELECT xwalk.person_id ,
                   'mobilize_id'||'_'|| user_id AS id_source_user_id ,
                   partic.email ,
                   'mobilize_id' AS id_source ,
                   event.mobilize_id AS id ,
                   partic.user_id ,
                   partic.user_firstname ,
                   partic.user_lastname ,
                   p.user_address_line_1 ,
                   p.user_address_line_2 ,
                   p.user_city ,
                   coalesce(p.user_state,partic.user_state_code) AS user_state ,
                   coalesce(partic.user_zip, p.voting_zip) AS user_zip ,
                   partic.user_phone ,
                   p.user_address_latitude ,
                   p.user_address_longitude ,
                   'attendee' AS user_role ,
                   partic.user_attended ,
                   partic.user_modified_date ,
                   event.event_address_line_1 ,
                   event.event_address_line_2 ,
                   event.event_city ,
                   coalesce(event.event_state_code,evxwalk.event_state) as event_state ,
                   event.event_postal_code ,
                   event.event_lat ,
                   event.event_lon ,
                   lower(event.event_type_source) as event_type_source ,
                   lower(event.event_title) AS event_title ,
                   to_date(TO_Timestamp(partic.event_start,'YYYY-MM-DD HH24:MI:SS'),'YYYY-MM-DD') AS event_date ,
                   cast(partic.event_start AS TIMESTAMP) AS event_timestamp ,
                   partic.event_status , 
                   evxwalk.ak_event_id , 
                   evxwalk.mobilize_id ,
                   evxwalk.event_type ,
                   evxwalk.mobilize_timeslot_id ,
                   evxwalk.van_event_van_id ,
                   evxwalk.van_timeslot_id ,
                   evxwalk.mob_shift_count ,
                   evxwalk.mob_shift_order ,
                   evxwalk.ak_title ,
                   evxwalk.ak_address1 ,
                   evxwalk.ak_venue ,
                   evxwalk.mobilize_address1 ,
                   evxwalk.mobilize_venue ,
                   evxwalk.ak_start_utc ,
                   evxwalk.mobilize_start_utc ,
                   evxwalk.van_location_id ,
                   evxwalk.van_location_name 
FROM
              (SELECT id,
                      user_id,
                      coalesce(user__given_name,given_name_at_signup) AS user_firstname,
                      coalesce(user__family_name,family_name_at_signup) AS user_lastname,
                      coalesce(user__email_address,email_at_signup) AS email,
                      coalesce(user__phone_number,phone_number_at_signup) AS user_phone,
                      coalesce(user__postal_code,postal_code_at_signup) AS user_zip,
                      TO_Timestamp(user__modified_date,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date,
                      state_code AS user_state_code,
                      event_id AS mobilize_id,
                      event_type AS mobilize_event_type,
                      start_date AS event_start,
                      organization_id AS event_organization_id,
                      organization__name AS event_organization_name,
                      status AS event_status,
                      attended AS user_attended
               FROM mobilize.participations_comprehensive) partic
            LEFT JOIN
              (SELECT id AS mobilize_id,
                      location__address_line_1 AS event_address_line_1,
                      location__address_line_2 AS event_address_line_2,
                      location__locality AS event_city,
                      location__region AS event_state_code,
                      location__country AS event_country,
                      location__postal_code AS event_postal_code,
                      location__lat AS event_lat,
                      location__lon AS event_lon,
                      event_type AS event_type_source, 
                      title AS event_title ,
                      event_campaign__slug AS event_campaign 
               FROM mobilize.events_comprehensive) event using(mobilize_id)
            LEFT JOIN
              (SELECT ak_event_id
                      ,mobilize_id
                      ,event_state
                      ,event_type
                      ,mobilize_timeslot_id
                      ,van_event_van_id
                      ,van_timeslot_id
                      ,mob_shift_count
                      ,mob_shift_order
                      ,ak_title
                      ,ak_address1
                      ,ak_venue
                      ,mobilize_address1
                      ,mobilize_venue
                      ,ak_start_utc
                      ,mobilize_start_utc
                      ,van_location_id
                      ,van_location_name FROM core_table_builds.events_xwalk) evxwalk on partic.mobilize_id::int = evxwalk.mobilize_id::int
            LEFT JOIN
              (SELECT person_id,
                      email
               FROM bernie_data_commons.master_xwalk) xwalk using(email)
            LEFT JOIN
              (SELECT person_id,
                      state_code AS user_state,
                      voting_street_address AS user_address_line_1,
                      voting_street_address_2 AS user_address_line_2,
                      voting_city AS user_city,
                      voting_zip,
                      voting_zip4,
                      voting_address_latitude AS user_address_latitude,
                      voting_address_longitude AS user_address_longitude
               FROM phoenix_analytics.person) p using(person_id) 
           )
         UNION ALL
           ( SELECT coalesce(xwalk_ak.person_id_1, xwalk.person_id_2) as person_id ,
                    'ak_event_id' ||'_'|| user_id AS id_source_user_id ,
                    akuser.email ,
                    'ak_event_id' AS id_source ,
                    event.ak_event_id AS id ,
                    akuser.user_id ,
                    akuser.user_firstname ,
                    akuser.user_lastname ,
                    coalesce(akuser.user_address_line_1, p.voting_street_address) AS user_address_line_1 ,
                    coalesce(akuser.user_address_line_2, p.voting_street_address_2) AS user_address_line_2 ,
                    coalesce(akuser.user_city, p.voting_city) AS user_city ,
                    coalesce(akuser.user_state, akuser.user_region, p.state_code) AS user_state ,
                    coalesce(akuser.user_zip, p.voting_zip, akuser.user_postal_code) AS user_zip ,
                    NULL AS user_phone ,
                    p.user_address_latitude ,
                    p.user_address_longitude ,
                    signup.user_role ,
                    signup.user_attended ,
                    akuser.user_modified_date ,
                    event.event_address_line_1 ,
                    event.event_address_line_2 ,
                    event.event_city ,
                    coalesce(event.event_state_code,evxwalk.event_state) as event_state ,
                    coalesce(event.event_postal_code,event.event_zip) AS event_postal_code ,
                    event.event_lat ,
                    event.event_lon ,
                    lower(campaign.event_name) AS event_type_source ,
                    lower(event.event_title) AS event_title ,
                    to_date(TO_Timestamp(event.event_start,'YYYY-MM-DD HH24:MI:SS'),'YYYY-MM-DD') AS event_date ,
                    cast(event.event_start AS TIMESTAMP) AS event_timestamp ,
                    event.event_status ,
                    evxwalk.ak_event_id , 
                    evxwalk.mobilize_id ,
                    evxwalk.event_type ,
                    evxwalk.mobilize_timeslot_id ,
                    evxwalk.van_event_van_id ,
                    evxwalk.van_timeslot_id ,
                    evxwalk.mob_shift_count ,
                    evxwalk.mob_shift_order ,
                    evxwalk.ak_title ,
                    evxwalk.ak_address1 ,
                    evxwalk.ak_venue ,
                    evxwalk.mobilize_address1 ,
                    evxwalk.mobilize_venue ,
                    evxwalk.ak_start_utc ,
                    evxwalk.mobilize_start_utc ,
                    evxwalk.van_location_id ,
                    evxwalk.van_location_name 
                    --,event_name
                    --,actionkit_id
                    --,signup_id
                    --,id_signupfield
                    --,event_venue
                    --,subscription_status
                    --,name_signupfield
                    --,value_signupfield
                    --,event_attendee_count
FROM
              -- (SELECT coalesce(xwalk_ak.person_id_1, xwalk.person_id_2) AS person_id, * FROM
                 (SELECT id AS user_id,
                         email,
                         first_name AS user_firstname,
                         last_name AS user_lastname,
                         address1 AS user_address_line_1,
                         address2 AS user_address_line_2,
                         city AS user_city,
                         STATE AS user_state,
                         region AS user_region,
                         postal AS user_postal_code,
                         zip AS user_zip,
                         country AS user_country,
                         TO_Timestamp(updated_at,'YYYY-MM-DD HH24:MI:SS') AS user_modified_date,
                         subscription_status
                  FROM ak_bernie.core_user) akuser
               INNER JOIN
                 (SELECT id AS signup_id,
                         user_id,
                         event_id AS ak_event_id,
                         ROLE AS user_role,
                         status AS user_status,
                         attended AS user_attended
                  FROM ak_bernie.events_eventsignup) signup using(user_id)
               INNER JOIN
                 (SELECT id AS id_signupfield,
                         parent_id AS signup_id,
                         name AS name_signupfield,
                         value AS value_signupfield
                  FROM ak_bernie.events_eventsignupfield) signupf using(signup_id)
               LEFT JOIN
                 (SELECT id AS ak_event_id,
                         address1 AS event_address_line_1,
                         address2 AS event_address_line_2,
                         city AS event_city,
                         coalesce(STATE,region) AS event_state_code,
                         postal AS event_postal_code,
                         zip AS event_zip,
                         country AS event_country,
                         longitude AS event_lon,
                         latitude AS event_lat,
                         starts_at AS event_start,
                         status AS event_status,
                         attendee_count AS event_attendee_count,
                         venue AS event_venue,
                         title AS event_title,
                         campaign_id
                  FROM ak_bernie.events_event) event using(ak_event_id)
                LEFT JOIN
                 (SELECT ak_event_id
                         ,mobilize_id
                         ,event_state
                         ,event_type
                         ,mobilize_timeslot_id
                         ,van_event_van_id
                         ,van_timeslot_id
                         ,mob_shift_count
                         ,mob_shift_order
                         ,ak_title
                         ,ak_address1
                         ,ak_venue
                         ,mobilize_address1
                         ,mobilize_venue
                         ,ak_start_utc
                         ,mobilize_start_utc
                         ,van_location_id
                         ,van_location_name 
                  FROM core_table_builds.events_xwalk) evxwalk on event.ak_event_id::int = evxwalk.ak_event_id::int
                LEFT JOIN
                 (SELECT id AS campaign_id,
                         --title AS event_title,
                         name AS event_name
                  FROM ak_bernie.events_campaign) campaign using(campaign_id)
               LEFT JOIN
                 (SELECT actionkit_id,
                         person_id AS person_id_1,
                         ROW_NUMBER() OVER(PARTITION BY actionkit_id ORDER BY row_id DESC) as rownum 
                  FROM bernie_data_commons.master_xwalk_ak) xwalk_ak ON xwalk_ak.actionkit_id = akuser.user_id AND xwalk_ak.rownum = 1
               LEFT JOIN
                 (SELECT person_id AS person_id_2,
                         email 
                  FROM bernie_data_commons.master_xwalk) xwalk using(email)
            LEFT JOIN
              (SELECT person_id,
                      state_code,
                      voting_street_address,
                      voting_street_address_2,
                      voting_city,
                      voting_zip,
                      voting_zip4,
                      voting_address_latitude AS user_address_latitude,
                      voting_address_longitude AS user_address_longitude
               FROM phoenix_analytics.person) p ON p.person_id = coalesce(xwalk_ak.person_id_1, xwalk.person_id_2)
              )
));


create temp table dedupe_ak_mobilize as
(select *, row_number() over (partition BY id_source_user_id ORDER BY user_modified_date DESC) AS dedupe from 
  (SELECT DISTINCT person_id,
                   id_source_user_id,
                   id_source,
                   user_id,
                   user_firstname,
                   user_lastname,
                   user_address_line_1,
                   user_address_line_2,
                   user_city,
                   user_state,
                   user_zip,
                   user_phone,
                   email,
                   user_modified_date,
                   user_address_latitude,
                   user_address_longitude
   FROM bernie_nmarchio2.events_ak_mobilize));


DROP TABLE IF EXISTS bernie_nmarchio2.events_ak_mobilize_ids;
CREATE TABLE bernie_nmarchio2.events_ak_mobilize_ids DISTKEY (person_id) AS
  (SELECT person_id ,
          id_source_user_id ,
          id_source ,
          user_id ,
          coalesce(user_firstname,user_firstname_2,user_firstname_3,user_firstname_4) as user_firstname , 
          coalesce(user_lastname,user_lastname_2,user_lastname_3,user_lastname_4) as user_lastname ,
          coalesce(user_address_line_1,user_address_line_1_2,user_address_line_1_3,user_address_line_1_4) as user_address_line_1 ,
          coalesce(user_address_line_2,user_address_line_2_2,user_address_line_2_3,user_address_line_2_4) as user_address_line_2 ,
          coalesce(user_city,user_city_2,user_city_3,user_city_4) as user_city ,
          coalesce(user_state,user_state_2,user_state_3,user_state_4) as user_state ,
          coalesce(user_zip,user_zip_2,user_zip_3,user_zip_4) as user_zip ,
          coalesce(user_phone,user_phone_2,user_phone_3,user_phone_4) as user_phone ,
          coalesce(email,email_2,email_3,email_4) as email ,
          coalesce(user_address_latitude,user_address_latitude_2,user_address_latitude_3,user_address_latitude_4) as user_address_latitude ,
          coalesce(user_address_longitude,user_address_longitude_2,user_address_longitude_3,user_address_longitude_4) as user_address_longitude ,
          user_modified_date 
   FROM
     (SELECT *
      FROM dedupe_ak_mobilize
      WHERE dedupe = 1)
   LEFT JOIN
     (SELECT id_source_user_id,
             user_firstname AS user_firstname_2,
             user_lastname AS user_lastname_2,
             user_address_line_1 AS user_address_line_1_2,
             user_address_line_2 AS user_address_line_2_2,
             user_city AS user_city_2,
             user_state AS user_state_2,
             user_zip AS user_zip_2,
             user_phone AS user_phone_2,
             email AS email_2,
             user_address_latitude AS user_address_latitude_2,
             user_address_longitude AS user_address_longitude_2
      FROM dedupe_ak_mobilize
      WHERE dedupe = 2) using(id_source_user_id)
   LEFT JOIN
     (SELECT id_source_user_id,
             user_firstname AS user_firstname_3,
             user_lastname AS user_lastname_3,
             user_address_line_1 AS user_address_line_1_3,
             user_address_line_2 AS user_address_line_2_3,
             user_city AS user_city_3,
             user_state AS user_state_3,
             user_zip AS user_zip_3,
             user_phone AS user_phone_3,
             email AS email_3,
             user_address_latitude AS user_address_latitude_3,
             user_address_longitude AS user_address_longitude_3
      FROM dedupe_ak_mobilize
      WHERE dedupe = 3) using(id_source_user_id)
   LEFT JOIN
     (SELECT id_source_user_id,
             user_firstname AS user_firstname_4,
             user_lastname AS user_lastname_4,
             user_address_line_1 AS user_address_line_1_4,
             user_address_line_2 AS user_address_line_2_4,
             user_city AS user_city_4,
             user_state AS user_state_4,
             user_zip AS user_zip_4,
             user_phone AS user_phone_4,
             email AS email_4,
             user_address_latitude AS user_address_latitude_4,
             user_address_longitude AS user_address_longitude_4
      FROM dedupe_ak_mobilize
      WHERE dedupe = 4) using(id_source_user_id));
