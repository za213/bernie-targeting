
--  Activism Sidetable (all individuals in Bern app, volunteers / attendees / RSVPs in ActionKit and Mobilize, shift volunteers and supporters in MyCampaign, and activism-related Survey Responses)
begin;
set wlm_query_slot_count to 2;
DROP TABLE IF EXISTS bernie_nmarchio2.base_activists;
CREATE TABLE bernie_nmarchio2.base_activists
distkey(person_id) 
sortkey(person_id) as
(select person_id,
	    voting_address_id,
	    coalesce(myc.mvp_myc ,0) as mvp_myc ,
        coalesce(myc.activist_myc ,0) as activist_myc ,
        coalesce(myc.canvass_myc ,0) as canvass_myc ,
        coalesce(myc.volunteer_myc ,0) as volunteer_myc ,
        coalesce(myc.constituencies_myc ,0) as constituencies_myc ,
        coalesce(myc.volunteer_shifts_myc ,0) as volunteer_shifts_myc ,
        coalesce(myc.student_myc ,0) as student_myc ,
        coalesce(myc.teacher_myc ,0) as teacher_myc ,
        coalesce(myc.labor_myc,0) as labor_myc,
        coalesce(bern.bernapp,0) as bernapp,
        coalesce(surveys.sticker_id ,0) as sticker_id ,
        coalesce(surveys.commit2caucus_id ,0) as commit2caucus_id ,
        coalesce(surveys.union_id ,0) as union_id ,
        coalesce(surveys.student_id ,0) as student_id ,
        coalesce(surveys.volunteer_yes_id,0) as volunteer_yes_id,
        coalesce(slack.slack_vol,0) as slack_vol,
        coalesce(akmob.rsvps,0) as rsvps,
        coalesce(akmob.akmob_rsvps_canvass ,0) as akmob_rsvps_canvass ,
        coalesce(akmob.akmob_rsvps_phonebank ,0) as akmob_rsvps_phonebank ,
        coalesce(akmob.akmob_rsvps_small_event ,0) as akmob_rsvps_small_event ,
        coalesce(akmob.akmob_rsvps_friend_to_friend ,0) as akmob_rsvps_friend_to_friend ,
        coalesce(akmob.akmob_rsvps_training ,0) as akmob_rsvps_training ,
        coalesce(akmob.akmob_rsvps_barnstorm ,0) as akmob_rsvps_barnstorm ,
        coalesce(akmob.akmob_rsvps_rally_town_hall ,0) as akmob_rsvps_rally_town_hall ,
        coalesce(akmob.akmob_rsvps_solidarity_action ,0) as akmob_rsvps_solidarity_action ,
        coalesce(akmob.attended,0) as attended,
        coalesce(akmob.akmob_attended_canvass ,0) as akmob_attended_canvass ,
        coalesce(akmob.akmob_attended_phonebank ,0) as akmob_attended_phonebank ,
        coalesce(akmob.akmob_attended_small_event ,0) as akmob_attended_small_event ,
        coalesce(akmob.akmob_attended_friend_to_friend ,0) as akmob_attended_friend_to_friend ,
        coalesce(akmob.akmob_attended_training ,0) as akmob_attended_training ,
        coalesce(akmob.akmob_attended_barnstorm ,0) as akmob_attended_barnstorm ,
        coalesce(akmob.akmob_attended_rally_town_hall ,0) as akmob_attended_rally_town_hall ,
        coalesce(akmob.akmob_attended_solidarity_action,0) as akmob_attended_solidarity_action,
        case 
        when rsvps >= 2 -- non-binary count
          or attended > 0 -- non-binary count
          or mvp_myc = 1
          or activist_myc = 1
          or canvass_myc = 1
          or volunteer_myc = 1
          or constituencies_myc = 1
          or volunteer_shifts_myc = 1
          or student_myc = 1
          or teacher_myc = 1
          or labor_myc = 1
          or bernapp = 1
          or sticker_id = 1
          or commit2caucus_id = 1
          or union_id = 1
          or student_id = 1
          or volunteer_yes_id = 1
          or slack_vol = 1 then 1
        else 0 end as activist_flag
 from 
(SELECT person_id::varchar,
        state_code,
        voting_address_id
      FROM phoenix_analytics.person
      WHERE is_deceased = FALSE
        AND reg_record_merged = FALSE
        AND reg_on_current_file = TRUE
        AND reg_voter_flag = TRUE ) p

    left join
    -- MyCampaign Activist Codes
    (select * from
     (SELECT mx.person_id::varchar(10) , 
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'MVP' then mx.person_id end) as mvp_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'Activist' then mx.person_id end) as activist_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'Canvassed' then mx.person_id end) as canvass_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'Volunteer' then mx.person_id end) as volunteer_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO ('%Latinx4Bern%|%LGBTQ 4 Bernie%|%Muslims4Bern%|%AfAm4Bernie%|%4 Bernie%|%BlackWomen4Bern%|%Share the Bern%|%Faith 4 Bernie%|%Women 4 Bernie%|%Veterans 4 Bernie%') then mx.person_id end) as constituencies_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO ('%Volunteer Level%|%Volunteer Team Lead%|%Volunteer Leader%|%Caucus Volunteer%|%Vol Yes%|%SuperVol%|%LatinX Vols%|%SuperVols%|%Canvass%|%Shift Compl%|%Knock Doors%|%Give ride to caucus%|%Phone Bank%|%HQ_Phonebank%') then mx.person_id end) as volunteer_shifts_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Student%' THEN mx.person_id END) AS student_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Teacher%' THEN mx.person_id END) AS teacher_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Labor%' THEN mx.person_id END) AS labor_myc
     FROM phoenix_demssanders20_vansync_derived.activist_myc ac
     LEFT JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id
     LEFT JOIN bernie_data_commons.master_xwalk_st_myc mx ON mx.myc_van_id = ac.myc_van_id AND mx.state_code = ac.state_code
     WHERE ac.committee_id = 73296 and mx.person_id is not null
     GROUP BY 1) where coalesce(mvp_myc ,activist_myc ,canvass_myc ,volunteer_myc ,constituencies_myc ,volunteer_shifts_myc ,student_myc ,teacher_myc ,labor_myc) <> 0) myc
    using(person_id) 
    
    left join
    -- Bern App
     (SELECT 
           person_id::varchar(10) ,
           count(distinct case when bern_id is not null or attempted_voter_lookup = 't' then person_id end) as bernapp
           FROM (SELECT * FROM bern_app.canvass_canvasser) canv
           LEFT JOIN
           (SELECT person_id::varchar(10), bern_id, bern_canvasser_id FROM bernie_data_commons.master_xwalk) xwalk ON canv.id = xwalk.bern_canvasser_id where person_id is not null
           GROUP BY 1) bern
     using(person_id)

    left join
    -- Survey Responses
    (select * from 
    (SELECT person_id::varchar(10) ,
            count(distinct CASE WHEN surveyresponseid IN (40) THEN person_id END) AS sticker_id ,
            count(distinct CASE WHEN surveyresponseid IN (99) THEN person_id END) AS commit2caucus_id ,
            count(distinct CASE WHEN surveyresponseid IN (10) THEN person_id END) AS union_id ,
            count(distinct CASE WHEN surveyresponseid IN (9,103,104) THEN person_id END) AS student_id ,
            count(distinct CASE WHEN surveyresponseid IN (32) THEN person_id END) AS volunteer_yes_id 
            FROM contacts.surveyresponses sr 
            INNER JOIN bernie_data_commons.ccj_dnc ccj using(contactcontact_id) 
            LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid) 
            WHERE person_id is not null group by 1) 
            where coalesce(sticker_id,commit2caucus_id,union_id,student_id,volunteer_yes_id) <> 0) surveys
    using(person_id) 
    
    left join
    -- Slack
    (SELECT person_id::varchar, 
             count(distinct CASE WHEN deleted = 'f' THEN 1 ELSE 0 END) AS slack_vol
             from 
             (SELECT id AS slack_id, name, deleted, profile_email FROM slack.vol_users) slack
             LEFT JOIN
             (SELECT person_id::varchar, email, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY email NULLS LAST) AS rownum FROM bernie_data_commons.master_xwalk) xwalk_master 
             ON xwalk_master.email = slack.profile_email AND xwalk_master.rownum = 1
             where person_id is not null
             GROUP BY 1) slack
    using(person_id) 
    
    left join
    -- ActionKit / Mobilize
    (SELECT person_id::varchar,
            count(person_id) as rsvps,
            count(CASE WHEN event_recode = 'canvass' THEN person_id END) AS akmob_rsvps_canvass ,
            count(CASE WHEN event_recode = 'phonebank' THEN person_id END) AS akmob_rsvps_phonebank ,
            count(CASE WHEN event_recode = 'small-event' THEN person_id END) AS akmob_rsvps_small_event ,
            count(CASE WHEN event_recode = 'friend-to-friend' THEN person_id END) AS akmob_rsvps_friend_to_friend ,
            count(CASE WHEN event_recode = 'training' THEN person_id END) AS akmob_rsvps_training ,
            count(CASE WHEN event_recode = 'barnstorm' THEN person_id END) AS akmob_rsvps_barnstorm ,
            count(CASE WHEN event_recode = 'rally-town-hall' THEN person_id END) AS akmob_rsvps_rally_town_hall ,
            count(CASE WHEN event_recode = 'solidarity-action' THEN person_id END) AS akmob_rsvps_solidarity_action ,
            count(case when attended = 't' then person_id end) as attended,
            count(CASE WHEN event_recode = 'canvass' and attended = 't' THEN person_id END) AS akmob_attended_canvass ,
            count(CASE WHEN event_recode = 'phonebank' and attended = 't' THEN person_id END) AS akmob_attended_phonebank ,
            count(CASE WHEN event_recode = 'small-event' and attended = 't' THEN person_id END) AS akmob_attended_small_event ,
            count(CASE WHEN event_recode = 'friend-to-friend' and attended = 't' THEN person_id END) AS akmob_attended_friend_to_friend ,
            count(CASE WHEN event_recode = 'training' and attended = 't' THEN person_id END) AS akmob_attended_training ,
            count(CASE WHEN event_recode = 'barnstorm' and attended = 't' THEN person_id END) AS akmob_attended_barnstorm ,
            count(CASE WHEN event_recode = 'rally-town-hall' and attended = 't' THEN person_id END) AS akmob_attended_rally_town_hall ,
            count(CASE WHEN event_recode = 'solidarity-action' and attended = 't' THEN person_id END) AS akmob_attended_solidarity_action 
           from (
    (select distinct person_id, event_recode, attended from -- mobilize_id, mobilize_event_id,
      (SELECT DISTINCT user_id::varchar(256) AS mobilize_id ,
                       event_id::varchar(256) AS mobilize_event_id ,
                       attended
       FROM mobilize.participations_comprehensive)
    LEFT JOIN
      (SELECT mobilize_id::varchar,
              person_id FROM (SELECT DISTINCT user_id AS mobilize_id, user__email_address::varchar(256) AS email FROM mobilize.participations_comprehensive WHERE mobilize_id IS NOT NULL AND email IS NOT NULL)
       LEFT JOIN
       (SELECT * FROM (SELECT person_id, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY person_id NULLS LAST) AS dupe FROM bernie_data_commons.master_xwalk) WHERE dupe = 1 AND person_id IS NOT NULL) using(email)) 
      using(mobilize_id) 
      LEFT JOIN 
      (select * from bernie_nmarchio2.base_akmobevents where mobilize_event_id is not null)
      using(mobilize_event_id))
    union all
    (select distinct person_id, event_recode, attended from -- actionkit_id, ak_event_id,
      (SELECT DISTINCT user_id::varchar(256) AS actionkit_id ,
                       event_id::varchar(256) AS ak_event_id ,
                       attended
       FROM ak_bernie.events_eventsignup)
    LEFT JOIN
      (SELECT DISTINCT person_id::varchar,
                       actionkit_id::varchar
       FROM bernie_data_commons.master_xwalk_ak WHERE person_id IS NOT NULL) 
      using(actionkit_id)
    LEFT JOIN 
      (select * from bernie_nmarchio2.base_akmobevents where ak_event_id is not null)
      using(ak_event_id))
    ) where person_id is not null and event_recode is not null group by 1) akmob
    using(person_id) where activist_flag = 1);
commit;

