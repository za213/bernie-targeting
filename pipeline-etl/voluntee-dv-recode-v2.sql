create temp table action_pop AS
(SELECT 
 p.person_id 
, CASE WHEN host = 1 THEN 1 
	   WHEN attendee = 1 THEN 1
       ELSE 0 END AS attendee_host_ever
, CASE WHEN attendee = 1 THEN 1 ELSE 0 END AS attendee_ever
, CASE when n_donations  > 0 then 1 else 0 end as donor_ever
, CASE when lifetime_value >= 27 then 1 else 0 end as donor_27plus_ever
, CASE when n_events_attended > 0 then 1 else 0 end as eventattend_ever
, CASE when  n_event_signups > 0 then 1 else 0 end as eventsignup_ever
FROM bernie_jshuman.donor_basetable base
LEFT JOIN phoenix_analytics.person p ON p.person_id = base.person_id
LEFT JOIN 
    (
        SELECT DISTINCT ca.user_id, 1 as host 
        FROM ak_bernie.core_action ca
        JOIN ak_bernie.core_eventcreateaction eca ON ca.id = eca.action_ptr_id
        JOIN ak_bernie.events_event ee ON eca.event_id = ee.id
        JOIN ak_bernie.events_campaign ec ON ee.campaign_id = ec.id
        WHERE TRIM(ec.title) <> 'Event with Bernie Sanders'
    ) hosts ON hosts.user_id = base.user_id
LEFT JOIN 
    (
        SELECT DISTINCT ca.user_id, 1 as attendee
        FROM ak_bernie.core_action ca
        JOIN ak_bernie.core_eventsignupaction ces ON ces.action_ptr_id = ca.id
        JOIN ak_bernie.events_eventsignup es ON es.id = ces.signup_id
        JOIN ak_bernie.events_event ee ON es.event_id = ee.id
        JOIN ak_bernie.events_campaign ec ON ee.campaign_id = ec.id
        WHERE TRIM(ec.title) <> 'Event with Bernie Sanders'
        AND es.role = 'attendee'
    ) attendees ON attendees.user_id = base.user_id
WHERE 
  (lifetime_value > 0 
or n_event_signups > 0 
or n_events_attended > 0
or n_donations > 0
or host > 0 
or attendee > 0));

DROP TABLE IF EXISTS bernie_nmarchio2.action_pop_dvs CASCADE;
CREATE TABLE bernie_nmarchio2.action_pop_dvs 
distkey(person_id) 
sortkey(person_id) AS
(select * from
 (select 
   person_id 
  ,attendee_host_ever
  ,attendee_ever
  ,donor_ever
  ,donor_27plus_ever
  ,eventattend_ever
  ,eventsignup_ever
  ,case when greatest(attendee_host_ever
                      ,attendee_ever
                      ,donor_ever
                      ,donor_27plus_ever
                      ,eventattend_ever
                      ,eventsignup_ever) > 0 then 1 else 0 end as action_ever
  from action_pop)
 union all
 (select 
   p.person_id 
  ,0 AS attendee_host_ever
  ,0 AS attendee_ever
  ,0 as donor_ever
  ,0 as donor_27plus_ever
  ,0 as eventattend_ever
  ,0 as eventsignup_ever
  ,0 as action_ever
  from phoenix_analytics.person p
  where p.person_id not in (select distinct person_id from action_pop)
  and random() < .1
  and is_deceased = false 
  and reg_record_merged = false 
  and reg_on_current_file = true 
  and reg_voter_flag = true
  limit 1000000)
  );
