
DROP TABLE IF EXISTS bernie_nmarchio2.engagement_analytics;
CREATE TABLE bernie_nmarchio2.engagement_analytics DISTKEY (person_id) AS 
(SELECT phx.person_id ,
    events.source_data ,
    events.user_id ,
    coalesce(events.user_email,slack.email) as user_email ,
    events.user_id_mobilize ,
    events.user_id_actionkit ,
    bern.bern_id ,
    bern.bern_canvasser_id ,
    slack.slack_vol ,
    events.attended ,
    events.signups ,
    events.attended_canvass ,
    events.signups_canvass ,
    events.attended_phonebank ,
    events.signups_phonebank ,
    events.attended_small_event ,
    events.signups_small_event ,
    events.attended_other ,
    events.signups_other ,
    events.attended_friend_to_friend ,
    events.signups_friend_to_friend ,
    events.attended_training ,
    events.signups_training ,
    events.attended_barnstorm ,
    events.signups_barnstorm ,
    events.attended_rally_town_hall ,
    events.signups_rally_town_hall ,
    events.attended_solidarity_action ,
    events.signups_solidarity_action ,
    events.active_10_days ,
    events.active_11_20_days ,
    events.active_21_30_days ,
    events.active_31_40_days ,
    events.active_41_50_days ,
    events.active_51_60_days ,
    events.active_61_70_days ,
    events.active_71_80_days ,
    events.active_81_90_days ,
    events.active_91_100_days ,
    events.active_over_100_days ,
    bern.bern_total_points,
    bern.bern_is_student,
    bern.bern_is_union,
    bern.bern_attempted_voter_lookup, 
    surveyresp.support_1_id ,
    surveyresp.support_1_2_id ,
    surveyresp.undecided_3_id ,
    surveyresp.persuaded_id ,
    surveyresp.bernie_id ,
    surveyresp.liz_joe_pete_support_id ,
    surveyresp.rest_of_field_support_id ,
    surveyresp.npp_yes_id ,
    surveyresp.sticker_id ,
    surveyresp.commit2caucus_id ,
    surveyresp.union_id ,
    surveyresp.student_id ,
    surveyresp.volunteer_yes_id ,
    surveyresp.volunteer_yes_maybe_id ,
    surveyresp.event_rsvp_yes_maybe ,
    surveyresp.has_will_donate ,
    spoke.spoke_support_1box ,
    spoke.spoke_persuasion_1plus ,
    spoke.spoke_persuasion_1minus ,
    spoke.spoke_persuasion_nochange ,
    spoke.support_init as spoke_support_init,
    spoke.support_final as spoke_support_final,
    spoke.support_change as spoke_support_change
FROM
(SELECT person_id::varchar(10) FROM phoenix_analytics.person where is_deceased = false and reg_record_merged = false and reg_on_current_file = true and reg_voter_flag = true) phx
FULL JOIN
  (SELECT *
   FROM event_analytics) events USING(person_id)
FULL JOIN
(SELECT person_id::varchar(10), CASE WHEN deleted = 'f' THEN 1 ELSE 0 END AS slack_vol, profile_email AS email
FROM
  (SELECT id AS slack_id, name, deleted, profile_email FROM slack.vol_users) slack
LEFT JOIN
  (SELECT person_id::varchar(10), email, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY email NULLS LAST) AS rownum
FROM bernie_data_commons.master_xwalk) xwalk_master ON (xwalk_master.email = slack.profile_email AND xwalk_master.rownum = 1)) slack USING(person_id)
FULL JOIN
(SELECT person_id::varchar(10) ,
       bern_id ,
       bern_canvasser_id ,
       total_points as bern_total_points ,
       CASE WHEN is_student = 't' then 1 else 0 end as bern_is_student ,
       CASE WHEN is_union = 't' then 1 else 0 end as bern_is_union ,
       CASE WHEN attempted_voter_lookup = 't' then 1 else 0 end as bern_attempted_voter_lookup
       FROM
  (SELECT * FROM bern_app.canvass_canvasser) canv
FULL JOIN
  (SELECT person_id::varchar(10),
          bern_id,
          bern_canvasser_id,
          ROW_NUMBER() OVER(PARTITION BY bern_id ORDER BY email NULLS LAST) AS rownum
FROM bernie_data_commons.master_xwalk) xwalk ON canv.id = xwalk.bern_id
AND xwalk.rownum = 1) bern USING(person_id)
LEFT JOIN (
SELECT *
FROM (
SELECT person_id::varchar(10) ,
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
           CASE WHEN surveyresponseid IN (35,36) THEN 1 ELSE 0 END AS event_rsvp_yes_maybe ,
           CASE WHEN surveyresponseid IN (37,38) THEN 1 ELSE 0 END AS has_will_donate ,
           row_number() over (partition BY person_id ORDER BY contactdate ASC) AS dup
FROM
  (SELECT *
   FROM contacts.surveyresponses) sr
JOIN bernie_data_commons.contactcontacts_joined ccj using(contactcontact_id)
LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid))
WHERE dup = 1
  AND person_id IS NOT NULL) surveyresp USING(person_id)
  LEFT JOIN (
  SELECT person_id::varchar(10),
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
JOIN bernie_data_commons.contactcontacts_joined ccj using(contactcontact_id)
LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid)
WHERE ccj.lalvoterid IS NOT NULL) WHERE dup = 1
  AND person_id IS NOT NULL) spoke USING(person_id)
  LEFT JOIN
 (SELECT person_id::varchar(10) ,
       sum(CASE WHEN activist_code_name SIMILAR TO '%2016 Donor%|%2020 Donor%|%2020 Rec Donor%|%Low Dollar Donor%' THEN 1 ELSE 0 END) AS donor_myc ,
       sum(CASE WHEN activist_code_name SIMILAR TO '%Volunteer Prospect%|%AK Volunteer Signup%|%VolProp%|%Vol Prop%|%VolProp%|%VolProp%|%Vol Prop%|%Border-Vol-Prospect%|%Caucus_Vol_Prospect%|%Online Vol Sign Up%|%16 Bern Vol Prospect%|%BSD Vol Signup%|%High_Prop_Vol_1014%|%AK LTE Signup%|%AK Text Signup%|%16 GOTV Signup%|%AK Rally Signup%|%AK Rally Signup%|%AK Email Signup%|%Web Signup%|%AK Student Signup%|%AK Tabling Signup%|%AK Spanish Signup%|%Web Commit Signup%|%AK Phonebank Signup%|%AK Barnstorm Signup%|%2016July 29th Signup%|%AK Pol. Event Signup%|%AK Sol. Event Signup%|%2020 Delegate Propec%|%OOS_Canvas_Interest%|%Ind4Bern%|%APIA4Bern%|%Latinx4Bern%|%LGBTQ 4 Bernie%|%Muslims4Bern%|%2016 AfAm4Bernie%|%2016 SMB 4 Bernie%|%BlackWomen4Bern%|%16 Latinx 4 Bernie%|%2016 Labor 4 Bernie%|%2016 - Labor4Bernie%|%16 Share the Bern%|%2016 Faith 4 Bernie%|%Students 4 Bernie%|%2016 Teachers4Bernie%|%Women 4 Bernie%|%16 Veterans 4 Bernie%|%Personal_Endorser%|%99 County Endorsees%|%Confirmed Endorser%|%Pol-Rural Endorsers%|%Endorser%|%2016 Supporter%' THEN 1 ELSE 0 END) AS volunteer_prospect_myc ,
       sum(CASE WHEN activist_code_name SIMILAR TO '%Labor%|%Labor Members%|%Labor CG Mem%|%Pol-Labor Endorsers%|%Pol-Labor%' THEN 1 ELSE 0 END) AS union_activist ,
       sum(CASE WHEN activist_code_name SIMILAR TO '%16 GV RallyRSVP%|%16 CHS Rally RSVP%|%16 Col Rally RSVP%|%2016 Winthrop RSVP%|%2016 Benedict RSVP%|%2016 Florence RSVP%|%2016 Sumter Ral RSVP%' THEN 1 ELSE 0 END) AS rsvp_myc ,
       sum(CASE WHEN activist_code_name SIMILAR TO '%Volunteer Level 1%|%Volunteer Level 2%|%Volunteer Level 3%|%Volunteer Team Lead%|%Volunteer Leader%|%Volunteer%|%OOS Volunteer%|%Caucus Volunteer%|%Vol. Team Member%|%2016 Vol Evnt Atnde%|%Past Vol Yes%|%2020 SuperVol%|%2020 LatinX Vols%|%2016 OOS NH Vol%|%2016 GV Rally Vol%|%2016 CHS Rally Vol%|%2016 SumterRallyVol%|%2016 ColumbiaRalVol%|%Team Maine SuperVols%|%2020 Superdelegates%|%2016 State Delegate%|%Delegate%|%2016 County Delegate%|%2016 Canvass%|%2016 Canvass Captain%|%Crowd_Canvas_BERN%|%AK Com. Canvass Host%|%IA_Shift_6-16--July%|%2016 DVC Shift Compl%|%2016 Knock Doors%|%16Door Knock Sign Up%|%Give ride to caucus%|%Phone Bank%|%HQ_Phonebank_yes%|%2016 Phone Bank SU%|%VT_HQ_Phonebank%' THEN 1 ELSE 0 END) AS shiftvolunteer_myc ,
       sum(CASE WHEN activist_code_name SIMILAR TO '%Student%|%Student UNR%|%Student UNLV%|%Student CSN%|%Student TMCC%|%Student High School%|%Student Bulk%|%Student NSC%|%Student_Leadership%|%Student WNC%|%Student SNC%|%16 Student - Comm%|%16 Student - HS%|%Grad Student%|%Prospective Student%|%High School Student%' THEN 1 ELSE 0 END) AS student_myc ,
       sum(CASE WHEN activist_code_name SIMILAR TO '%Teacher%' THEN 1 ELSE 0 END) AS teacher_myc
FROM phoenix_demssanders20_vansync_derived.activist_myc ac
INNER JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id
LEFT JOIN bernie_data_commons.master_xwalk mx ON mx.myc_van_id = ac.myc_van_id
AND mx.state_code = ac.state_code
GROUP BY 1) USING(person_id));
