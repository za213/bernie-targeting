create temp table action_pop AS
(SELECT 
  person_id 
, CASE WHEN host = 1 THEN 1 WHEN attendee = 1 THEN 1 ELSE 0 END AS attendee_or_host
, CASE WHEN attendee = 1 THEN 1 ELSE 0 END AS attendee
, case when kickoff_party > 0 then 1 else 0 end as kickoff_party_attendee
, case when canvasser > 0 then 1 else 0 end as canvasser_attendee 
, case when phonebank > 0 then 1 else 0 end as phonebank_attendee
, case when rally_barnstorm_event > 0 then 1 else 0 end as rally_barnstorm_attendee
, case when rally_barnstorm_event > 0 then 1 when kickoff_party_attendee > 0 then 1 else 0 end as kickoff_party_rally_barnstorm_attendee
, case when canvasser > 0 then 1 when phonebank > 0 then 1 else 0 end as canvasser_phonebank_attendee
, CASE when n_donations  > 0 then 1 else 0 end as donor_1plus_times
, CASE when lifetime_value >= 27 then 1 else 0 end as donor_27plus_usd 
from 
(select 
p.person_id
,sum(host) as host
,sum(attendee) as attendee
,sum(kickoff_party) as kickoff_party
,sum(canvasser) as canvasser
,sum(phonebank ) as phonebank
,sum(rally_barnstorm_event) as rally_barnstorm_event
,sum(n_donations) as n_donations
,sum(lifetime_value) as lifetime_value
FROM bernie_jshuman.donor_basetable base
INNER JOIN phoenix_analytics.person p ON p.person_id = base.person_id
LEFT JOIN 
    (
        SELECT DISTINCT ca.user_id, 1 as host 
        FROM ak_bernie.core_action ca
        JOIN ak_bernie.core_eventcreateaction eca ON ca.id = eca.action_ptr_id
        JOIN ak_bernie.events_event ee ON eca.event_id = ee.id
        JOIN ak_bernie.events_campaign ec ON ee.campaign_id = ec.id
    ) hosts ON hosts.user_id = base.user_id
LEFT JOIN 
    (SELECT ca.user_id, 1 as attendee,
                              
	sum(case when ec.title ilike '%Canvass%'
	or ec.title ilike '%Bernie on the Ballot%'
	or ec.title ilike '%Bernie-Journey%' then 1 else 0 end) as canvasser
	
	,sum(case when ec.title ilike '%Volunteer Training%'
	or ec.title ilike '%Plan to Win Party%'
	or ec.title ilike '%House Party%'
	or ec.title ilike '%Debate Watch Party%'
	or ec.title ilike '%Organizing Kickoff%' 
	or ec.title ilike '%Office Opening%' then 1 else 0 end) as kickoff_party
	
	,sum(case when ec.title ilike '%Phonebank%'
	or ec.title ilike '%Call for Bernie%'
	or ec.title ilike '%Friend-to-Friend%'
	or ec.title ilike '%Postcards for Bernie%' then 1 else 0 end) as phonebank
	
	,sum(case when ec.title ilike '%Solidarity Event%'
	or ec.title ilike '%Event with Bernie Sanders%'
	or ec.title ilike '%Bernie 2020 Event%'
	or ec.title ilike '%Bernie 2020%'
	or ec.title ilike '%Bernie Town Hall%'
	or ec.title ilike '%Bernie Rally%'
	or ec.title ilike '%Official Events%'
	or ec.title ilike '%Rally%'
	or ec.title ilike '%Get Out The Vote for %'
	or ec.title ilike '%Barnstorm%' then 1 else 0 end) as rally_barnstorm_event
                                       
	FROM ak_bernie.core_action ca
	JOIN ak_bernie.core_eventsignupaction ces ON ces.action_ptr_id = ca.id
	JOIN ak_bernie.events_eventsignup es ON es.id = ces.signup_id
	JOIN ak_bernie.events_event ee ON es.event_id = ee.id
	JOIN ak_bernie.events_campaign ec ON ee.campaign_id = ec.id
	where es.role = 'attendee' group by 1,2 ) attendees ON attendees.user_id = base.user_id 
WHERE (lifetime_value > 0 or n_donations > 0 or host > 0 or attendee > 0 and p.person_id is not null)  
group by 1));

DROP TABLE IF EXISTS bernie_nmarchio2.action_pop_dvs CASCADE;
CREATE TABLE bernie_nmarchio2.action_pop_dvs 
distkey(person_id) 
sortkey(person_id) AS
(select * from
 (select 
   person_id 
,attendee_or_host
,attendee
,kickoff_party_attendee
,canvasser_attendee 
,phonebank_attendee
,rally_barnstorm_attendee
,kickoff_party_rally_barnstorm_attendee
,canvasser_phonebank_attendee
,donor_1plus_times
,donor_27plus_usd
  ,case when greatest(attendee_or_host
,attendee
,kickoff_party_attendee
,canvasser_attendee 
,phonebank_attendee
,rally_barnstorm_attendee
,kickoff_party_rally_barnstorm_attendee
,canvasser_phonebank_attendee
,donor_1plus_times
,donor_27plus_usd) > 0 then 1 else 0 end as bernie_action
,0 as nontarget_group 
  from action_pop)
 union all
 (select 
   p.person_id 
,0 as attendee_or_host
,0 as attendee
,0 as kickoff_party_attendee
,0 as canvasser_attendee 
,0 as phonebank_attendee
,0 as rally_barnstorm_attendee
,0 as kickoff_party_rally_barnstorm_attendee
,0 as canvasser_phonebank_attendee
,0 as donor_1plus_times
,0 as donor_27plus_usd
,0 as bernie_action 
,1 as nontarget_group 
  from phoenix_analytics.person p
  where p.person_id not in (select distinct person_id from action_pop)
  and random() < .1
  and is_deceased = false 
  and reg_record_merged = false 
  and reg_on_current_file = true 
  and reg_voter_flag = true
  limit 1000000)
  );
