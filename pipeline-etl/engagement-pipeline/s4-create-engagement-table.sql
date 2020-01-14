
-- ActionKit-Mobilize
DROP TABLE IF EXISTS bernie_nmarchio2.universe_actionkit_mobilize;
CREATE TABLE bernie_nmarchio2.universe_actionkit_mobilize AS
(SELECT source_data ,
       user_id ,
       person_id ,
       user_email ,
       user_id_mobilize ,
       user_id_actionkit ,
       SUM(CASE WHEN user_attended = 't' THEN 1 ELSE 0 END) AS attended ,
       SUM(CASE WHEN user_attended = 't' OR user_attended = 'f' THEN 1 ELSE 0 END) AS signups ,
       SUM(CASE WHEN event_type_v2 = 'canvass' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_canvass ,
       SUM(CASE WHEN event_type_v2 = 'canvass' THEN 1 ELSE 0 END) AS signups_canvass ,
       SUM(CASE WHEN event_type_v2 = 'phonebank' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_phonebank ,
       SUM(CASE WHEN event_type_v2 = 'phonebank' THEN 1 ELSE 0 END) AS signups_phonebank ,
       SUM(CASE WHEN event_type_v2 = 'small-event' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_small_event ,
       SUM(CASE WHEN event_type_v2 = 'small-event' THEN 1 ELSE 0 END) AS signups_small_event ,
       SUM(CASE WHEN event_type_v2 = 'other' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_other ,
       SUM(CASE WHEN event_type_v2 = 'other' THEN 1 ELSE 0 END) AS signups_other ,
       SUM(CASE WHEN event_type_v2 = 'friend-to-friend' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_friend_to_friend ,
       SUM(CASE WHEN event_type_v2 = 'friend-to-friend' THEN 1 ELSE 0 END) AS signups_friend_to_friend ,
       SUM(CASE WHEN event_type_v2 = 'training' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_training ,
       SUM(CASE WHEN event_type_v2 = 'training' THEN 1 ELSE 0 END) AS signups_training ,
       SUM(CASE WHEN event_type_v2 = 'barnstorm' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_barnstorm ,
       SUM(CASE WHEN event_type_v2 = 'barnstorm' THEN 1 ELSE 0 END) AS signups_barnstorm ,
       SUM(CASE WHEN event_type_v2 = 'rally-town-hall' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_rally_town_hall ,
       SUM(CASE WHEN event_type_v2 = 'rally-town-hall' THEN 1 ELSE 0 END) AS signups_rally_town_hall ,
       SUM(CASE WHEN event_type_v2 = 'solidarity-action' AND user_attended = 't' THEN 1 ELSE 0 END) AS attended_solidarity_action ,
       SUM(CASE WHEN event_type_v2 = 'solidarity-action' THEN 1 ELSE 0 END) AS signups_solidarity_action ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) <= 10 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_10_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 11 AND 20 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_11_20_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 21 AND 30 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_21_30_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 31 AND 40 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_31_40_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 41 AND 50 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_41_50_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 51 AND 60 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_51_60_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 61 AND 70 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_61_70_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 71 AND 80 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_71_80_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 81 AND 90 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_81_90_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) BETWEEN 91 AND 100 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_91_100_days ,
       SUM(CASE WHEN datediff(d, TO_DATE(user_modified_date, 'YYYY-MM-DD'), CURRENT_DATE) >= 101 AND user_attended = 't' THEN 1 ELSE 0 END) AS active_over_100_days 
       FROM
(SELECT * FROM 
(SELECT * FROM
   (SELECT * FROM bernie_nmarchio2.events_signups WHERE source_data = 'actionkit') sign_ak
 LEFT JOIN
   (SELECT *, ROW_NUMBER() OVER(PARTITION BY ak_event_id ORDER BY mobilize_id NULLS LAST) AS rownum FROM bernie_nmarchio2.events_details) evnt_ak ON (sign_ak.ak_event_id = evnt_ak.ak_event_id AND evnt_ak.rownum = 1))
UNION ALL 
(SELECT * FROM
   (SELECT * FROM bernie_nmarchio2.events_signups WHERE source_data = 'mobilize') sign_mob
 LEFT JOIN
   (SELECT *, ROW_NUMBER() OVER(PARTITION BY mobilize_id ORDER BY ak_event_id NULLS LAST) AS rownum FROM bernie_nmarchio2.events_details) evnt_mob ON (sign_mob.mobilize_id = evnt_mob.mobilize_id AND evnt_mob.rownum = 1)))
GROUP BY 1,2,3,4,5,6);

-- MyCampaign Activist Codes
DROP TABLE IF EXISTS bernie_nmarchio2.universe_myc;
CREATE TABLE bernie_nmarchio2.universe_myc AS
 (SELECT mx.person_id::varchar(10) ,
         mx.actionkit_id ,
         mx.email ,
         mx.st_myc_van_id ,
         SUM(CASE WHEN ac_lookup.activist_code_type = 'MVP' then 1 else 0 end) as mvp_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type = 'Activist' then 1 else 0 end) as activist_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type = 'Canvassed' then 1 else 0 end) as canvass_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type = 'Volunteer' then 1 else 0 end) as volunteer_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type = 'Email' or ac_lookup.activist_code_name SIMILAR TO '%AK Email Signup%' then 1 else 0 end) as email_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type IN ('Donors','Donor') or ac_lookup.activist_code_name SIMILAR TO '%2020 Donor%|%2020 Rec Donor%|%Low Dollar Donor%' then 1 else 0 end) as donors_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type = 'Events' then 1 else 0 end) as events_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type = 'Legacy' or ac_lookup.activist_code_name SIMILAR TO ('%2016%|%16%') then 1 else 0 end) as legacy_2016_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type IN ('Political','Constituency','Constituency/Issue','Issue') or ac_lookup.activist_code_name SIMILAR TO ('%Latinx4Bern%|%LGBTQ 4 Bernie%|%Muslims4Bern%|%AfAm4Bernie%|%4 Bernie%|%BlackWomen4Bern%|%Share the Bern%|%Faith 4 Bernie%|%Women 4 Bernie%|%Veterans 4 Bernie%') then 1 else 0 end) as constituencies_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type IN ('Staff','Elected Officials','Party Officials','Candidate') or ac_lookup.activist_code_name SIMILAR TO ('%Endorser%|%Endorsee%') then 1 else 0 end ) as officials_staff_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_type IN ('Contacts','Out of State','Other') then 1 else 0 end) as other_activist_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_name SIMILAR TO ('%Volunteer Level%|%Volunteer Team Lead%|%Volunteer Leader%|%Caucus Volunteer%|%Vol Yes%|%SuperVol%|%LatinX Vols%|%SuperVols%|%Canvass%|%Shift Compl%|%Knock Doors%|%Give ride to caucus%|%Phone Bank%|%HQ_Phonebank%') then 1 else 0 end) as volunteer_shifts_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_name SIMILAR TO ('%Prospect%|%Volunteer Signup%|%Vol Sign Up%|%Prop_Vol%|%Door Knock Sign Up%|%VolProp%|%Vol Prop%|%Web Commit Signup%|%AK Volunteer Signup%|%AK LTE Signup%|%AK Text Signup%|%AK Spanish Signup%|%AK Tabling Signup%|%AK Phonebank Signup%') then 1 else 0 end) as volunteer_signup_myc ,
         SUM(CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%RallyRSVP%|%Rally RSVP%|% RSVP%|%Ral RSVP%|%Rally Signup%|%Barnstorm Signup%' THEN 1 ELSE 0 END) AS rally_rsvp_myc ,
         sum(CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Student%' THEN 1 ELSE 0 END) AS student_myc ,
         sum(CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Teacher%' THEN 1 ELSE 0 END) AS teacher_myc ,
         sum(CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Labor%' THEN 1 ELSE 0 END) AS labor_myc ,
         sum(CASE WHEN ac_lookup.activist_code_name IS NOT NULL THEN 1 ELSE 0 END) AS present_in_myc
 FROM phoenix_demssanders20_vansync_derived.activist_myc ac
 LEFT JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id
 LEFT JOIN bernie_data_commons.master_xwalk_st_myc mx ON mx.myc_van_id = ac.myc_van_id AND mx.state_code = ac.state_code
 WHERE ac.committee_id = 73296
 GROUP BY 1, 2, 3, 4);

-- Bern App
DROP TABLE IF EXISTS bernie_nmarchio2.universe_bern;
CREATE TABLE bernie_nmarchio2.universe_bern AS
 (SELECT person_id::varchar(10) ,
       bern_id ,
       bern_canvasser_id ,
       total_points as bern_total_points ,
       CASE WHEN is_student = 't' then 1 else 0 end as bern_is_student ,
       CASE WHEN is_union = 't' then 1 else 0 end as bern_is_union ,
       CASE WHEN attempted_voter_lookup = 't' then 1 else 0 end as bern_attempted_voter_lookup
       FROM
  (SELECT * FROM bern_app.canvass_canvasser) canv
 LEFT JOIN
  (SELECT person_id::varchar(10),
          bern_id,
          bern_canvasser_id,
          ROW_NUMBER() OVER(PARTITION BY bern_id ORDER BY email NULLS LAST) AS rownum
 FROM bernie_data_commons.master_xwalk) xwalk ON canv.id = xwalk.bern_id AND xwalk.rownum = 1);

-- Survey Responses
DROP TABLE IF EXISTS bernie_nmarchio2.universe_surveyresponse;
CREATE TABLE bernie_nmarchio2.universe_surveyresponse AS
(SELECT * FROM (
 SELECT person_id::varchar(10) ,
        actionkit_id ,
        st_myc_van_id ,
        CASE WHEN surveyresponseid IN (1) THEN 1 ELSE 0 END AS support_1_id ,
        CASE WHEN surveyresponseid IN (1,2) THEN 1 ELSE 0 END AS support_1_2_id ,
        CASE WHEN surveyresponseid IN (3) THEN 1 ELSE 0 END AS undecided_3_id ,
        CASE WHEN surveyresponseid IN (96) THEN 1 WHEN surveyresponseid IN (97,98) THEN 0 ELSE NULL END AS persuaded_id ,
        CASE WHEN surveyresponseid IN (83) THEN 1 ELSE 0 END AS bernie_id ,
        CASE WHEN surveyresponseid IN (13,15,16) THEN 1 ELSE 0 END AS liz_joe_pete_support_id ,
        CASE WHEN surveyresponseid IN (17,18,19,20,21,22,23,24,25,26,27,28,29,30,95) THEN 1 ELSE 0 END AS rest_of_field_support_id ,
        CASE WHEN surveyresponseid IN (105) THEN 1 WHEN surveyresponseid IN (106,107) THEN 0 ELSE NULL END AS npp_yes_id ,
        CASE WHEN surveyresponseid IN (40) THEN 1 WHEN surveyresponseid IN (106,107) THEN 0 ELSE NULL END AS sticker_id ,
        CASE WHEN surveyresponseid IN (99) THEN 1 WHEN surveyresponseid IN (100,101) THEN 0 ELSE NULL END AS commit2caucus_id ,
        CASE WHEN surveyresponseid IN (10) THEN 1 ELSE 0 END AS union_id ,
        CASE WHEN surveyresponseid IN (9,103,104) THEN 1 ELSE 0 END AS student_id ,
        CASE WHEN surveyresponseid IN (32) THEN 1 WHEN surveyresponseid IN (33) THEN 0 ELSE NULL END AS volunteer_yes_id ,
        CASE WHEN surveyresponseid IN (32,34,94) THEN 1 WHEN surveyresponseid IN (33) THEN 0 ELSE NULL END AS volunteer_yes_maybe_id ,
        CASE WHEN surveyresponseid IN (35,36) THEN 1 ELSE 0 END AS event_rsvp_yes_maybe_id ,
        CASE WHEN surveyresponseid IN (37,38) THEN 1 ELSE 0 END AS has_will_donate_id ,
        row_number() over (partition BY person_id ORDER BY contactdate ASC) AS dup
 FROM
  (SELECT * FROM contacts.surveyresponses) sr INNER JOIN bernie_data_commons.contactcontacts_joined ccj using(contactcontact_id) LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid)) WHERE dup = 1);

-- Spoke
DROP TABLE IF EXISTS bernie_nmarchio2.universe_spoke;
CREATE TABLE bernie_nmarchio2.universe_spoke AS
(SELECT person_id::varchar(10),
        CASE WHEN support_init = 1 THEN 1 ELSE 0 END AS spoke_support_1box,
        CASE WHEN support_change >= 1 THEN 1 WHEN support_init = 1 THEN NULL ELSE 0 END AS spoke_persuasion_1plus,
        CASE WHEN support_change <= -1 THEN 1 WHEN support_init = 5 THEN NULL ELSE 0 END AS spoke_persuasion_1minus,
        CASE WHEN support_change = 0 THEN 1 ELSE 0 END AS spoke_persuasion_nochange,
        support_init,
        support_final,
        support_change 
        FROM
   (SELECT person_id,
           sr.*,
           srt.*,
           contactdate,
           row_number() over (partition BY person_id ORDER BY contactdate ASC) AS dup,
           json_extract_path_text(lower(surveyresponseopen), 'support_init') AS support_init,
           json_extract_path_text(lower(surveyresponseopen), 'support_final') AS support_final,
           json_extract_path_text(lower(surveyresponseopen), 'support_change') AS support_change
           FROM
   (SELECT *
    FROM contacts.surveyresponses
    WHERE surveyquestionid = 28) sr
 INNER JOIN bernie_data_commons.contactcontacts_joined ccj using(contactcontact_id)
 LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid)
 WHERE ccj.lalvoterid IS NOT NULL) WHERE dup = 1 AND person_id IS NOT NULL); 

-- Slack
DROP TABLE IF EXISTS bernie_nmarchio2.universe_slack;
CREATE TABLE bernie_nmarchio2.universe_slack AS
 (SELECT person_id::varchar(10), st_myc_van_id, CASE WHEN deleted = 'f' THEN 1 ELSE 0 END AS slack_vol, profile_email AS email
 FROM
  (SELECT id AS slack_id, name, deleted, profile_email FROM slack.vol_users) slack
 LEFT JOIN
  (SELECT person_id::varchar(10), st_myc_van_id, email, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY email NULLS LAST) AS rownum
 FROM bernie_data_commons.master_xwalk) xwalk_master ON (xwalk_master.email = slack.profile_email AND xwalk_master.rownum = 1));

-- Engagement Universe
DROP TABLE IF EXISTS bernie_nmarchio2.universe_engagement;
CREATE TABLE bernie_nmarchio2.universe_engagement AS 
(SELECT 
 coalesce(xwalk.person_id, akm_1.person_id, akm_2.person_id) AS person_id
,xwalk.bern_id 
,coalesce(slack_1.email,slack_2.email,akm_1.user_email,akm_2.user_email,akm_3.user_email,xwalk.email) AS email 
,xwalk.st_myc_van_id
,coalesce(akm_1.user_id_mobilize, akm_2.user_id_mobilize, akm_3.user_id_mobilize) AS mobilize_id
,xwalk.actionkit_id
,coalesce(akm_1.attended,akm_2.attended,akm_3.attended) AS attended
,coalesce(akm_1.signups,akm_2.signups,akm_3.signups) AS signups
,coalesce(akm_1.attended_canvass,akm_2.attended_canvass,akm_3.attended_canvass) AS attended_canvass
,coalesce(akm_1.signups_canvass,akm_2.signups_canvass,akm_3.signups_canvass) AS signups_canvass
,coalesce(akm_1.attended_phonebank,akm_2.attended_phonebank,akm_3.attended_phonebank) AS attended_phonebank
,coalesce(akm_1.signups_phonebank,akm_2.signups_phonebank,akm_3.signups_phonebank) AS signups_phonebank
,coalesce(akm_1.attended_small_event,akm_2.attended_small_event,akm_3.attended_small_event) AS attended_small_event
,coalesce(akm_1.signups_small_event,akm_2.signups_small_event,akm_3.signups_small_event) AS signups_small_event
,coalesce(akm_1.attended_other,akm_2.attended_other,akm_3.attended_other) AS attended_other
,coalesce(akm_1.signups_other,akm_2.signups_other,akm_3.signups_other) AS signups_other
,coalesce(akm_1.attended_friend_to_friend,akm_2.attended_friend_to_friend,akm_3.attended_friend_to_friend) AS attended_friend_to_friend
,coalesce(akm_1.signups_friend_to_friend,akm_2.signups_friend_to_friend,akm_3.signups_friend_to_friend) AS signups_friend_to_friend
,coalesce(akm_1.attended_training,akm_2.attended_training,akm_3.attended_training) AS attended_training
,coalesce(akm_1.signups_training,akm_2.signups_training,akm_3.signups_training) AS signups_training
,coalesce(akm_1.attended_barnstorm,akm_2.attended_barnstorm,akm_3.attended_barnstorm) AS attended_barnstorm
,coalesce(akm_1.signups_barnstorm,akm_2.signups_barnstorm,akm_3.signups_barnstorm) AS signups_barnstorm
,coalesce(akm_1.attended_rally_town_hall,akm_2.attended_rally_town_hall,akm_3.attended_rally_town_hall) AS attended_rally_town_hall
,coalesce(akm_1.signups_rally_town_hall,akm_2.signups_rally_town_hall,akm_3.signups_rally_town_hall) AS signups_rally_town_hall
,coalesce(akm_1.attended_solidarity_action,akm_2.attended_solidarity_action,akm_3.attended_solidarity_action) AS attended_solidarity_action
,coalesce(akm_1.signups_solidarity_action,akm_2.signups_solidarity_action,akm_3.signups_solidarity_action) AS signups_solidarity_action
,coalesce(akm_1.active_10_days,akm_2.active_10_days,akm_3.active_10_days) AS active_10_days
,coalesce(akm_1.active_11_20_days,akm_2.active_11_20_days,akm_3.active_11_20_days) AS active_11_20_days
,coalesce(akm_1.active_21_30_days,akm_2.active_21_30_days,akm_3.active_21_30_days) AS active_21_30_days
,coalesce(akm_1.active_31_40_days,akm_2.active_31_40_days,akm_3.active_31_40_days) AS active_31_40_days
,coalesce(akm_1.active_41_50_days,akm_2.active_41_50_days,akm_3.active_41_50_days) AS active_41_50_days
,coalesce(akm_1.active_51_60_days,akm_2.active_51_60_days,akm_3.active_51_60_days) AS active_51_60_days
,coalesce(akm_1.active_61_70_days,akm_2.active_61_70_days,akm_3.active_61_70_days) AS active_61_70_days
,coalesce(akm_1.active_71_80_days,akm_2.active_71_80_days,akm_3.active_71_80_days) AS active_71_80_days
,coalesce(akm_1.active_81_90_days,akm_2.active_81_90_days,akm_3.active_81_90_days) AS active_81_90_days
,coalesce(akm_1.active_91_100_days,akm_2.active_91_100_days,akm_3.active_91_100_days) AS active_91_100_days
,coalesce(akm_1.active_over_100_days,akm_2.active_over_100_days,akm_3.active_over_100_days) AS active_over_100_days

,myc.mvp_myc
,myc.activist_myc
,myc.canvass_myc
,myc.volunteer_myc
,myc.email_myc
,myc.donors_myc
,myc.events_myc
,myc.legacy_2016_myc
,myc.constituencies_myc
,myc.officials_staff_myc
,myc.other_activist_myc
,myc.volunteer_shifts_myc
,myc.volunteer_signup_myc
,myc.rally_rsvp_myc
,myc.student_myc
,myc.teacher_myc
,myc.labor_myc
,myc.present_in_myc

,bern.bern_total_points
,bern.bern_is_student
,bern.bern_is_union
,bern.bern_attempted_voter_lookup

,coalesce(slack_1.slack_vol,slack_2.slack_vol) AS slack_vol

,surveys.support_1_id
,surveys.support_1_2_id
,surveys.undecided_3_id
,surveys.persuaded_id
,surveys.bernie_id
,surveys.liz_joe_pete_support_id
,surveys.rest_of_field_support_id
,surveys.npp_yes_id
,surveys.sticker_id
,surveys.commit2caucus_id
,surveys.union_id
,surveys.student_id
,surveys.volunteer_yes_id
,surveys.volunteer_yes_maybe_id
,surveys.event_rsvp_yes_maybe_id
,surveys.has_will_donate_id

,spoke.spoke_support_1box
,spoke.spoke_persuasion_1plus
,spoke.spoke_persuasion_1minus
,spoke.spoke_persuasion_nochange
,spoke.support_init
,spoke.support_final
,spoke.support_change

 FROM (

-- Crosswalk
(SELECT * FROM bernie_data_commons.master_xwalk) xwalk
-- Survey responses
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_surveyresponse WHERE person_id IS NOT NULL) surveys
ON surveys.person_id = xwalk.person_id
-- Spoke
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_spoke WHERE person_id IS NOT NULL) spoke
ON spoke.person_id = xwalk.person_id
-- MyCampaign Activists
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_myc WHERE st_myc_van_id IS NOT NULL) myc
ON myc.st_myc_van_id = xwalk.st_myc_van_id
-- Bern App
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_bern WHERE bern_id IS NOT NULL) bern
ON bern.bern_id = xwalk.bern_id
-- Slack
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_slack) slack_1
ON (slack_1.st_myc_van_id = xwalk.st_myc_van_id AND slack_1.st_myc_van_id IS NOT NULL) 
FULL JOIN
(SELECT * FROM bernie_nmarchio2.universe_slack) slack_2
ON (slack_2.email = xwalk.email AND slack_2.st_myc_van_id IS NULL AND slack_2.email IS NOT NULL)
-- ActionKit
LEFT JOIN 
(SELECT * FROM bernie_nmarchio2.universe_actionkit_mobilize) akm_1
ON (akm_1.user_id_actionkit = xwalk.actionkit_id AND akm_1.user_id_actionkit IS NOT NULL) 
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_actionkit_mobilize) akm_2
ON (akm_2.person_id = xwalk.person_id AND akm_2.user_id_actionkit IS NULL AND akm_2.person_id IS NOT NULL) 
FULL JOIN
(SELECT * FROM bernie_nmarchio2.universe_actionkit_mobilize) akm_3
ON (akm_3.user_email = xwalk.email AND akm_3.user_id_actionkit IS NULL AND akm_3.person_id IS NULL AND akm_3.user_email IS NOT NULL) 
)
WHERE COALESCE(surveys.person_id::VARCHAR,spoke.person_id::VARCHAR,myc.st_myc_van_id::VARCHAR,bern.bern_id::VARCHAR,slack_1.st_myc_van_id::VARCHAR,slack_2.email::VARCHAR,akm_1.user_id_actionkit::VARCHAR,akm_2.person_id::VARCHAR,akm_3.user_email::VARCHAR) IS NOT NULL
);


