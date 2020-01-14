
-- ActionKit-Mobilize
DROP TABLE IF EXISTS bernie_nmarchio2.universe_actionkit_mobilize;
CREATE TABLE bernie_nmarchio2.universe_actionkit_mobilize AS
(SELECT source_data ,
       user_id ,
       person_id ,
       user_email ,
       user_id_mobilize ,
       user_id_actionkit ,
       1 AS actionkit_mobilize_universe , 
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
         1 AS mycampaign_universe , 
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
       1 AS bern_universe ,
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
        1 AS survey_universe ,
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
		1 AS spoke_universe ,
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
 (SELECT person_id::varchar(10), 
         st_myc_van_id, 
         1 AS slack_universe,
         CASE WHEN deleted = 'f' THEN 1 ELSE 0 END AS slack_vol, 
         profile_email AS email
 FROM
  (SELECT id AS slack_id, name, deleted, profile_email FROM slack.vol_users) slack
 LEFT JOIN
  (SELECT person_id::varchar(10), st_myc_van_id, email, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY email NULLS LAST) AS rownum
 FROM bernie_data_commons.master_xwalk) xwalk_master ON (xwalk_master.email = slack.profile_email AND xwalk_master.rownum = 1));

-- Engagement Universe
DROP TABLE IF EXISTS bernie_nmarchio2.universe_engagement;
CREATE TABLE bernie_nmarchio2.universe_engagement AS 
(SELECT 
 COALESCE(xwalk.person_id::VARCHAR, akm_1.person_id::VARCHAR, akm_2.person_id::VARCHAR) AS person_id
,xwalk.bern_id 
,COALESCE(slack_1.email,slack_2.email,akm_1.user_email,akm_2.user_email,akm_3.user_email,xwalk.email) AS email 
,xwalk.st_myc_van_id
,COALESCE(akm_1.user_id_mobilize::VARCHAR, akm_2.user_id_mobilize::VARCHAR, akm_3.user_id_mobilize::VARCHAR) AS mobilize_id
,xwalk.actionkit_id

,COALESCE(akm_1.actionkit_mobilize_universe,akm_2.actionkit_mobilize_universe,akm_3.actionkit_mobilize_universe,0) AS actionkit_mobilize_universe
,COALESCE(akm_1.attended,akm_2.attended,akm_3.attended,0) AS attended
,COALESCE(akm_1.signups,akm_2.signups,akm_3.signups,0) AS signups
,COALESCE(akm_1.attended_canvass,akm_2.attended_canvass,akm_3.attended_canvass,0) AS attended_canvass
,COALESCE(akm_1.signups_canvass,akm_2.signups_canvass,akm_3.signups_canvass,0) AS signups_canvass
,COALESCE(akm_1.attended_phonebank,akm_2.attended_phonebank,akm_3.attended_phonebank,0) AS attended_phonebank
,COALESCE(akm_1.signups_phonebank,akm_2.signups_phonebank,akm_3.signups_phonebank,0) AS signups_phonebank
,COALESCE(akm_1.attended_small_event,akm_2.attended_small_event,akm_3.attended_small_event,0) AS attended_small_event
,COALESCE(akm_1.signups_small_event,akm_2.signups_small_event,akm_3.signups_small_event,0) AS signups_small_event
,COALESCE(akm_1.attended_other,akm_2.attended_other,akm_3.attended_other,0) AS attended_other
,COALESCE(akm_1.signups_other,akm_2.signups_other,akm_3.signups_other,0) AS signups_other
,COALESCE(akm_1.attended_friend_to_friend,akm_2.attended_friend_to_friend,akm_3.attended_friend_to_friend,0) AS attended_friend_to_friend
,COALESCE(akm_1.signups_friend_to_friend,akm_2.signups_friend_to_friend,akm_3.signups_friend_to_friend,0) AS signups_friend_to_friend
,COALESCE(akm_1.attended_training,akm_2.attended_training,akm_3.attended_training,0) AS attended_training
,COALESCE(akm_1.signups_training,akm_2.signups_training,akm_3.signups_training,0) AS signups_training
,COALESCE(akm_1.attended_barnstorm,akm_2.attended_barnstorm,akm_3.attended_barnstorm,0) AS attended_barnstorm
,COALESCE(akm_1.signups_barnstorm,akm_2.signups_barnstorm,akm_3.signups_barnstorm,0) AS signups_barnstorm
,COALESCE(akm_1.attended_rally_town_hall,akm_2.attended_rally_town_hall,akm_3.attended_rally_town_hall,0) AS attended_rally_town_hall
,COALESCE(akm_1.signups_rally_town_hall,akm_2.signups_rally_town_hall,akm_3.signups_rally_town_hall,0) AS signups_rally_town_hall
,COALESCE(akm_1.attended_solidarity_action,akm_2.attended_solidarity_action,akm_3.attended_solidarity_action,0) AS attended_solidarity_action
,COALESCE(akm_1.signups_solidarity_action,akm_2.signups_solidarity_action,akm_3.signups_solidarity_action,0) AS signups_solidarity_action
,COALESCE(akm_1.active_10_days,akm_2.active_10_days,akm_3.active_10_days,0) AS active_10_days
,COALESCE(akm_1.active_11_20_days,akm_2.active_11_20_days,akm_3.active_11_20_days,0) AS active_11_20_days
,COALESCE(akm_1.active_21_30_days,akm_2.active_21_30_days,akm_3.active_21_30_days,0) AS active_21_30_days
,COALESCE(akm_1.active_31_40_days,akm_2.active_31_40_days,akm_3.active_31_40_days,0) AS active_31_40_days
,COALESCE(akm_1.active_41_50_days,akm_2.active_41_50_days,akm_3.active_41_50_days,0) AS active_41_50_days
,COALESCE(akm_1.active_51_60_days,akm_2.active_51_60_days,akm_3.active_51_60_days,0) AS active_51_60_days
,COALESCE(akm_1.active_61_70_days,akm_2.active_61_70_days,akm_3.active_61_70_days,0) AS active_61_70_days
,COALESCE(akm_1.active_71_80_days,akm_2.active_71_80_days,akm_3.active_71_80_days,0) AS active_71_80_days
,COALESCE(akm_1.active_81_90_days,akm_2.active_81_90_days,akm_3.active_81_90_days,0) AS active_81_90_days
,COALESCE(akm_1.active_91_100_days,akm_2.active_91_100_days,akm_3.active_91_100_days,0) AS active_91_100_days
,COALESCE(akm_1.active_over_100_days,akm_2.active_over_100_days,akm_3.active_over_100_days,0) AS active_over_100_days

,COALESCE(myc.mycampaign_universe,0) AS mycampaign_universe
,COALESCE(myc.mvp_myc,0) AS mvp_myc
,COALESCE(myc.activist_myc,0) AS activist_myc
,COALESCE(myc.canvass_myc,0) AS canvass_myc
,COALESCE(myc.volunteer_myc,0) AS volunteer_myc
,COALESCE(myc.email_myc,0) AS email_myc
,COALESCE(myc.donors_myc,0) AS donors_myc
,COALESCE(myc.events_myc,0) AS events_myc
,COALESCE(myc.legacy_2016_myc,0) AS legacy_2016_myc
,COALESCE(myc.constituencies_myc,0) AS constituencies_myc
,COALESCE(myc.officials_staff_myc,0) AS officials_staff_myc
,COALESCE(myc.other_activist_myc,0) AS other_activist_myc
,COALESCE(myc.volunteer_shifts_myc,0) AS volunteer_shifts_myc
,COALESCE(myc.volunteer_signup_myc,0) AS volunteer_signup_myc
,COALESCE(myc.rally_rsvp_myc,0) AS rally_rsvp_myc
,COALESCE(myc.student_myc,0) AS student_myc
,COALESCE(myc.teacher_myc,0) AS teacher_myc
,COALESCE(myc.labor_myc,0) AS labor_myc
,COALESCE(myc.present_in_myc,0) AS present_in_myc

,COALESCE(bern.bern_universe,0) AS bern_universe
,COALESCE(bern.bern_total_points,0) AS bern_total_points
,COALESCE(bern.bern_is_student,0) AS bern_is_student
,COALESCE(bern.bern_is_union,0) AS bern_is_union
,COALESCE(bern.bern_attempted_voter_lookup,0) AS bern_attempted_voter_lookup

,COALESCE(slack_1.slack_universe,slack_1.slack_universe,0) AS slack_universe
,COALESCE(slack_1.slack_vol,slack_2.slack_vol,0) AS slack_vol

,COALESCE(surveys.survey_universe,0) AS survey_universe
,COALESCE(surveys.support_1_id,0) AS support_1_id
,COALESCE(surveys.support_1_2_id,0) AS support_1_2_id
,COALESCE(surveys.undecided_3_id,0) AS undecided_3_id
,COALESCE(surveys.persuaded_id,0) AS persuaded_id
,COALESCE(surveys.bernie_id,0) AS bernie_id
,COALESCE(surveys.liz_joe_pete_support_id,0) AS liz_joe_pete_support_id
,COALESCE(surveys.rest_of_field_support_id,0) AS rest_of_field_support_id
,COALESCE(surveys.npp_yes_id,0) AS npp_yes_id
,COALESCE(surveys.sticker_id,0) AS sticker_id
,COALESCE(surveys.commit2caucus_id,0) AS commit2caucus_id
,COALESCE(surveys.union_id,0) AS union_id
,COALESCE(surveys.student_id,0) AS student_id
,COALESCE(surveys.volunteer_yes_id,0) AS volunteer_yes_id
,COALESCE(surveys.volunteer_yes_maybe_id,0) AS volunteer_yes_maybe_id
,COALESCE(surveys.event_rsvp_yes_maybe_id,0) AS event_rsvp_yes_maybe_id
,COALESCE(surveys.has_will_donate_id,0) AS has_will_donate_id

,COALESCE(spoke.spoke_universe,0) AS spoke_universe
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
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_slack) slack_2
ON (slack_2.email = xwalk.email AND slack_2.st_myc_van_id IS NULL AND slack_2.email IS NOT NULL)
-- ActionKit
LEFT JOIN 
(SELECT * FROM bernie_nmarchio2.universe_actionkit_mobilize) akm_1
ON (akm_1.user_id_actionkit = xwalk.actionkit_id AND akm_1.user_id_actionkit IS NOT NULL) 
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_actionkit_mobilize) akm_2
ON (akm_2.person_id = xwalk.person_id AND akm_2.user_id_actionkit IS NULL AND akm_2.person_id IS NOT NULL) 
LEFT JOIN
(SELECT * FROM bernie_nmarchio2.universe_actionkit_mobilize) akm_3
ON (akm_3.user_email = xwalk.email AND akm_3.user_id_actionkit IS NULL AND akm_3.person_id IS NULL AND akm_3.user_email IS NOT NULL) 
)
WHERE COALESCE(akm_1.actionkit_mobilize_universe::int,akm_2.actionkit_mobilize_universe::int,akm_3.actionkit_mobilize_universe::int,myc.mycampaign_universe::int,bern.bern_universe::int,surveys.survey_universe::int,spoke.spoke_universe::int,slack_1.slack_universe::int,slack_2.slack_universe::int) IS NOT NULL
);
