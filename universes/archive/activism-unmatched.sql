
select x.person_id,
        x.state_code,
        x.myc_van_id,
        x.actionkit_id,
        x.jsonid_encoded,

         case when x.bern_canvasser_id is not null then 1 else 0 end as bernapp,

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

  FROM bernie_data_commons.master_xwalk x 
  
  LEFT JOIN (SELECT person_id::varchar,
              state_code,
              voting_address_id
      FROM phoenix_analytics.person
      WHERE is_deceased = FALSE
        AND reg_record_merged = FALSE
        AND reg_on_current_file = TRUE
        AND reg_voter_flag = TRUE ) p on p.person_id = x.person_id
    left join
    -- MyCampaign Activist Codes
    (select * from
     (SELECT ac.myc_van_id,
              ac.state_code,
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'MVP' then ac.myc_van_id end) as mvp_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'Activist' then ac.myc_van_id end) as activist_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'Canvassed' then ac.myc_van_id end) as canvass_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_type = 'Volunteer' then ac.myc_van_id end) as volunteer_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO ('%Latinx4Bern%|%LGBTQ 4 Bernie%|%Muslims4Bern%|%AfAm4Bernie%|%4 Bernie%|%BlackWomen4Bern%|%Share the Bern%|%Faith 4 Bernie%|%Women 4 Bernie%|%Veterans 4 Bernie%') then ac.myc_van_id end) as constituencies_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO ('%Volunteer Level%|%Volunteer Team Lead%|%Volunteer Leader%|%Caucus Volunteer%|%Vol Yes%|%SuperVol%|%LatinX Vols%|%SuperVols%|%Canvass%|%Shift Compl%|%Knock Doors%|%Give ride to caucus%|%Phone Bank%|%HQ_Phonebank%') then ac.myc_van_id end) as volunteer_shifts_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Student%' THEN ac.myc_van_id END) AS student_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Teacher%' THEN ac.myc_van_id END) AS teacher_myc ,
             count(distinct CASE WHEN ac_lookup.activist_code_name SIMILAR TO '%Labor%' THEN ac.myc_van_id END) AS labor_myc
     FROM phoenix_demssanders20_vansync_derived.activist_myc ac
     LEFT JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id
     WHERE ac.committee_id = 73296 
     GROUP BY 1,2) where coalesce(mvp_myc ,activist_myc ,canvass_myc ,volunteer_myc ,constituencies_myc ,volunteer_shifts_myc ,student_myc ,teacher_myc ,labor_myc) <> 0) myc
        ON x.myc_van_id = myc.myc_van_id AND x.state_code = myc.state_code
    
    left join
    -- Survey Responses
    (select * from 
    (SELECT jsonid_encoded,
            count(distinct CASE WHEN surveyresponseid IN (40) THEN jsonid_encoded END) AS sticker_id ,
            count(distinct CASE WHEN surveyresponseid IN (99) THEN jsonid_encoded END) AS commit2caucus_id ,
            count(distinct CASE WHEN surveyresponseid IN (10) THEN jsonid_encoded END) AS union_id ,
            count(distinct CASE WHEN surveyresponseid IN (9,103,104) THEN jsonid_encoded END) AS student_id ,
            count(distinct CASE WHEN surveyresponseid IN (32) THEN jsonid_encoded END) AS volunteer_yes_id  
            FROM contacts.surveyresponses sr 
            INNER JOIN bernie_data_commons.contactcontacts_joined ccj using(contactcontact_id) 
            LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid) 
            WHERE jsonid_encoded is not null 
            group by 1) 
            where coalesce(sticker_id,commit2caucus_id,union_id,student_id,volunteer_yes_id) <> 0
            ) surveys
    using(jsonid_encoded) 
   
    left join
    -- Slack
    (SELECT   profile_email as email,
             count(distinct CASE WHEN deleted = 'f' THEN profile_email END) AS slack_vol
             from 
             (SELECT id AS slack_id, name, deleted, profile_email FROM slack.vol_users) slack
             GROUP BY 1) slack
    USING(email)
   
    left join
    -- ActionKit / Mobilize
    (SELECT actionkit_id::bigint,
        count(distinct ak_event_id) as total_rsvps,
        count(distinct case when attended = 't' then ak_event_id end) as total_attended,
           count(distinct CASE WHEN event_recode = 'canvass' and attended = 't' THEN actionkit_id END) AS akmob_attended_canvass ,
           count(distinct CASE WHEN event_recode = 'phonebank' and attended = 't' THEN actionkit_id END) AS akmob_attended_phonebank ,
           count(distinct CASE WHEN event_recode = 'small-event' and attended = 't' THEN actionkit_id END) AS akmob_attended_small_event ,
           count(distinct CASE WHEN event_recode = 'friend-to-friend' and attended = 't' THEN actionkit_id END) AS akmob_attended_friend_to_friend ,
           count(distinct CASE WHEN event_recode = 'training' and attended = 't' THEN actionkit_id END) AS akmob_attended_training ,
           count(distinct CASE WHEN event_recode = 'barnstorm' and attended = 't' THEN actionkit_id END) AS akmob_attended_barnstorm ,
           count(distinct CASE WHEN event_recode = 'rally-town-hall' and attended = 't' THEN actionkit_id END) AS akmob_attended_rally_town_hall ,
           count(distinct CASE WHEN event_recode = 'solidarity-action' and attended = 't' THEN actionkit_id END) AS akmob_attended_solidarity_action 
           from (
    select distinct actionkit_id, ak_event_id, event_recode, attended from
      (SELECT DISTINCT user_id::varchar(256) AS actionkit_id ,
                       event_id::varchar(256) AS ak_event_id ,
                        attended
       FROM ak_bernie.events_eventsignup) 
       JOIN bernie_nmarchio2.akmobevents using(ak_event_id)
    ) 
    where event_recode is not null
    group by 1) akmob
        using(actionkit_id) 
    
    where activist_flag = 1;
