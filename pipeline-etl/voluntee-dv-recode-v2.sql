DROP TABLE IF EXISTS bernie_nmarchio2.volunteer_dvs CASCADE;
CREATE TABLE bernie_nmarchio2.volunteer_dvs distkey(person_id) sortkey(person_id) AS
(SELECT base.*
, CASE WHEN hosts.user_id IS NOT NULL THEN 1 
	   WHEN attendees.user_id IS NOT NULL THEN 1
       ELSE 0 END AS attendee_host_ever
, CASE WHEN attendees.user_id IS NOT NULL THEN 1 ELSE 0 END AS attendee_ever
FROM bernie_jshuman.donor_basetable base
LEFT JOIN phoenix_analytics.person p ON p.person_id = base.person_id
LEFT JOIN 
    (
        SELECT DISTINCT ca.user_id
        FROM ak_bernie.core_action ca
        JOIN ak_bernie.core_eventcreateaction eca ON ca.id = eca.action_ptr_id
        JOIN ak_bernie.events_event ee ON eca.event_id = ee.id
        JOIN ak_bernie.events_campaign ec ON ee.campaign_id = ec.id
        WHERE TRIM(ec.title) <> 'Event with Bernie Sanders'
    ) hosts ON hosts.user_id = base.user_id
LEFT JOIN 
    (
        SELECT DISTINCT ca.user_id
        FROM ak_bernie.core_action ca
        JOIN ak_bernie.core_eventsignupaction ces ON ces.action_ptr_id = ca.id
        JOIN ak_bernie.events_eventsignup es ON es.id = ces.signup_id
        JOIN ak_bernie.events_event ee ON es.event_id = ee.id
        JOIN ak_bernie.events_campaign ec ON ee.campaign_id = ec.id
        WHERE TRIM(ec.title) <> 'Event with Bernie Sanders'
        AND es.role = 'attendee'
    ) attendees ON attendees.user_id = base.user_id
JOIN bernie_jshuman.fifteen_day_actives fda on base.user_id = fda.user_id
ORDER BY RANDOM()
LIMIT 250000)
;
