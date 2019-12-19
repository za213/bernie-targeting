create temp table action_pop AS
(SELECT 
 p.person_id 
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
WHERE (lifetime_value > 0 or n_donations > 0 or host > 0 or attendee > 0));

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
  from phoenix_analytics.person p
  where p.person_id not in (select distinct person_id from action_pop)
  and random() < .1
  and is_deceased = false 
  and reg_record_merged = false 
  and reg_on_current_file = true 
  and reg_voter_flag = true
  limit 1000000)
  );


*/
,case when 
activist_code_name ilike '%2016 Donor%' or
activist_code_name ilike '%2020 Donor%' or
activist_code_name ilike '%2020 Rec Donor%' or
activist_code_name ilike '%*Low Dollar Donor*%' then 1 else 0 end as donor_myc

,case when
activist_code_name ilike '%Volunteer Prospect%' or
activist_code_name ilike '%AK Volunteer Signup%' or
activist_code_name ilike '%VolProp[35,50]%' or
activist_code_name ilike '%Vol Prop. 30+%' or
activist_code_name ilike '%VolProp>50%' or
activist_code_name ilike '%VolProp[37.5,50.0]%' or
activist_code_name ilike '%Vol Prop. 40+%' or
activist_code_name ilike '%Vol Prop. 60+%' or
activist_code_name ilike '%Border-Vol-Prospect%' or
activist_code_name ilike '%Caucus_Vol_Prospect%' or
activist_code_name ilike '%Online Vol Sign Up%' or
activist_code_name ilike '%16 Bern Vol Prospect%' or
activist_code_name ilike '%2016: BSD Vol Signup%' or
activist_code_name ilike '%High_Prop_Vol_1014%' or
activist_code_name ilike '%AK LTE Signup%' or
activist_code_name ilike '%AK Text Signup%' or
activist_code_name ilike '%16 GOTV Signup%' or
activist_code_name ilike '%AK Rally Signup%' or
activist_code_name ilike '%AK Rally Signup(Dep)%' or
activist_code_name ilike '%AK Email Signup%' or
activist_code_name ilike '%2016: Web Signup%' or
activist_code_name ilike '%AK Student Signup%' or
activist_code_name ilike '%AK Tabling Signup%' or
activist_code_name ilike '%AK Spanish Signup%' or
activist_code_name ilike '%*Web Commit Signup%' or
activist_code_name ilike '%AK Phonebank Signup%' or
activist_code_name ilike '%AK Barnstorm Signup%' or
activist_code_name ilike '%2016July 29th Signup%' or
activist_code_name ilike '%AK Pol. Event Signup%' or
activist_code_name ilike '%AK Sol. Event Signup%' or
activist_code_name ilike '%2020 Delegate Propec%' or
activist_code_name ilike '%OOS_Canvas_Interest%' or
activist_code_name ilike '%Ind4Bern%' or
activist_code_name ilike '%APIA4Bern%' or
activist_code_name ilike '%Latinx4Bern%' or
activist_code_name ilike '%LGBTQ 4 Bernie%' or
activist_code_name ilike '%Muslims4Bern%' or
activist_code_name ilike '%2016 AfAm4Bernie%' or
activist_code_name ilike '%2016 SMB 4 Bernie%' or
activist_code_name ilike '%BlackWomen4Bern%' or
activist_code_name ilike '%16 Latinx 4 Bernie%' or
activist_code_name ilike '%2016 Labor 4 Bernie%' or
activist_code_name ilike '%2016 - Labor4Bernie%' or
activist_code_name ilike '%16 Share the Bern%' or
activist_code_name ilike '%2016 Faith 4 Bernie%' or
activist_code_name ilike '%16:Students 4 Bernie%' or
activist_code_name ilike '%2016 Teachers4Bernie%' or
activist_code_name ilike '%2016: Women 4 Bernie%' or
activist_code_name ilike '%16 Veterans 4 Bernie%' or
activist_code_name ilike '%Personal_Endorser%' or
activist_code_name ilike '%99 County Endorsees%' or
activist_code_name ilike '%Confirmed Endorser%' or
activist_code_name ilike '%Pol-Rural Endorsers%' or
activist_code_name ilike '%Endorser%' or
activist_code_name ilike '%2016 Supporter%' then 1 else 0 end as volunteer_prospect_myc

,case when
activist_code_name ilike '%Labor%' or
activist_code_name ilike '%Labor Members%' or
activist_code_name ilike '%Labor CG Mem%' or
activist_code_name ilike '%Pol-Labor Endorsers%' or
activist_code_name ilike '%Pol-Labor%' or

,case when
activist_code_name ilike '%Phone Bank%' or
activist_code_name ilike '%HQ_Phonebank_yes%' or
activist_code_name ilike '%2016 Phone Bank SU%' or
activist_code_name ilike '%VT_HQ_Phonebank%' then 1 else 0 end as phonebank_myc

,case when
activist_code_name ilike '%16 GV RallyRSVP%' or
activist_code_name ilike '%16 CHS Rally RSVP%' or
activist_code_name ilike '%16 Col Rally RSVP%' or
activist_code_name ilike '%2016 Winthrop RSVP%' or
activist_code_name ilike '%2016 Benedict RSVP%' or
activist_code_name ilike '%2016 Florence RSVP%' or
activist_code_name ilike '%2016 Sumter Ral RSVP%' then 1 else 0 end as rsvp_myc

,case when
activist_code_name ilike '%Volunteer Level 1%' or
activist_code_name ilike '%Volunteer Level 2%' or
activist_code_name ilike '%Volunteer Level 3%' or
activist_code_name ilike '%Volunteer Team Lead%' or
activist_code_name ilike '%Volunteer Leader%' or
activist_code_name ilike '%Volunteer: Mailroom%' or
activist_code_name ilike '%OOS Volunteer%' or
activist_code_name ilike '%Caucus Volunteer%' or
activist_code_name ilike '%Vol. Team Member%' or
activist_code_name ilike '%2016 Vol Evnt Atnde%' or
activist_code_name ilike '%Past Vol Yes%' or
activist_code_name ilike '%2020 SuperVol%' or
activist_code_name ilike '%2020 LatinX Vols%' or
activist_code_name ilike '%2016 OOS NH Vol%' or
activist_code_name ilike '%2016 GV Rally Vol%' or
activist_code_name ilike '%2016 CHS Rally Vol%' or
activist_code_name ilike '%2016 SumterRallyVol%' or
activist_code_name ilike '%2016 ColumbiaRalVol%' or
activist_code_name ilike '%Team Maine SuperVols%' or
activist_code_name ilike '%2020 Superdelegates%' or
activist_code_name ilike '%2016 State Delegate%' or
activist_code_name ilike '%2016 Nat'l Delegate%' or
activist_code_name ilike '%2016 County Delegate%' or
activist_code_name ilike '%2016 Canvass%' or
activist_code_name ilike '%2016 Canvass Captain%' or
activist_code_name ilike '%Crowd_Canvas_BERN%' or
activist_code_name ilike '%AK Com. Canvass Host%' or
activist_code_name ilike '%IA_Shift_6-16--July%' or
activist_code_name ilike '%2016 DVC Shift Compl%' or
activist_code_name ilike '%2016 Knock Doors%' or
activist_code_name ilike '%16Door Knock Sign Up%' or
activist_code_name ilike '%Give ride to caucus%' then 1 else 0 end as shiftvolunteer_myc

,case when 
activist_code_name ilike '%Student%' or
activist_code_name ilike '%Student UNR%' or
activist_code_name ilike '%Student UNLV%' or
activist_code_name ilike '%Student CSN%' or
activist_code_name ilike '%Student TMCC%' or
activist_code_name ilike '%Student High School%' or
activist_code_name ilike '%Student Bulk%' or
activist_code_name ilike '%Student NSC%' or
activist_code_name ilike '%Student_Leadership%' or
activist_code_name ilike '%Student WNC%' or
activist_code_name ilike '%Student SNC%' or
activist_code_name ilike '%16 Student - Comm%' or
activist_code_name ilike '%16 Student - HS%' or
activist_code_name ilike '%Grad Student%' or
activist_code_name ilike '%Prospective Student%' or
activist_code_name ilike '%High School Student%' then 1 else 0 end as student_myc

,case when 
activist_code_name ilike '%Teacher%' then 1 else 0 end as teacher_myc
/*
