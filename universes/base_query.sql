
-- Owner: Nico Marchio 
-- Entered Production: 2020-02-05
-- Last Update: 2020-02-12
-- Purpose: Builds data tables to inform voter targeting, list cutting, and voter contact

------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

-- Field / Third Party IDs Side Table: all 1-5 support IDs collected to date -- note, the HARDCODED values which are subject to change as holdout universes shift
begin;
DROP TABLE IF EXISTS bernie_nmarchio2.base_validation;
CREATE TABLE bernie_nmarchio2.base_validation
distkey(person_id) 
sortkey(person_id) as
(SELECT * FROM      
	(SELECT person_id::varchar
      FROM phoenix_analytics.person
      WHERE is_deceased = 'f'
        AND reg_record_merged = 'f'
        AND reg_on_current_file = 't'
        AND reg_voter_flag = 't') p
   LEFT JOIN
-- FIELD IDS
    (select person_id::varchar
            ,ccj_contactdate
            ,coalesce(ccj_contact_made,0) as ccj_contact_made
            ,coalesce(ccj_negative_result,0) as ccj_negative_result
            ,coalesce(ccj_id_1,0) as ccj_id_1
            ,coalesce(ccj_id_1_2,0) as ccj_id_1_2
            ,coalesce(ccj_id_2,0) as ccj_id_2
            ,coalesce(ccj_id_3,0) as ccj_id_3
            ,coalesce(ccj_id_4,0) as ccj_id_4
            ,coalesce(ccj_id_5,0) as ccj_id_5
            ,coalesce(ccj_id_1_2_3_4_5,0) as ccj_id_1_2_3_4_5
            ,case 
            WHEN validtime = 1 then 1
            WHEN holdout = 1 THEN 1
            ELSE 0 END AS ccj_holdout_id
    FROM 
    (SELECT person_id::varchar 
            ,contactdate
            ,voter_state
            ,max(TO_DATE(contactdate, 'YYYY-MM-DD')) OVER (PARTITION BY person_id) AS ccj_contactdate
            ,case when
             datediff(d, '2020-01-17', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state = 'MN' or
             datediff(d, '2020-01-22', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state IN ('TN','AR','MO','AL','KY','GA','MS') or
             datediff(d, '2020-02-05', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state NOT IN ('MN','TN','AR','MO','AL','KY','GA','MS','NH','NV','IA','SC') THEN 1 END as validtime -- HARDCODE
            ,CASE WHEN resultcode IN ('Canvassed', 'Do Not Contact', 'Refused', 'Call Back', 'Language Barrier', 'Hostile', 'Come Back', 'Cultivation', 'Refused Contact', 'Spanish', 'Other', 'Not Interested') THEN 1 ELSE 0 END AS ccj_contact_made 
            ,CASE WHEN resultcode IN ('Do Not Contact','Hostile','Refused','Refused Contact') OR (support_int = 4 OR support_int = 5) THEN 1 ELSE 0 END AS ccj_negative_result 
            ,CASE WHEN support_int = 1 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1 
            ,CASE WHEN support_int IN (1,2) AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1_2 
            ,CASE WHEN support_int = 2 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_2 
            ,CASE WHEN support_int = 3 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_3 
            ,CASE WHEN support_int = 4 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_4 
            ,CASE WHEN support_int = 5 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_5 
            ,CASE WHEN support_int IN (1,2,3,4,5) AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1_2_3_4_5
            FROM bernie_data_commons.ccj_dnc 
            WHERE unique_id_flag = 1 AND person_id IS NOT NULL) ccj
            LEFT JOIN 
            (select person_id::varchar, 1 as holdout from haystaq.set_no_current where set_no=3) ho -- HARDCODE
            using(person_id) where person_id is not null and contactdate = ccj_contactdate) ccj
    using(person_id)
-- THIRD PARTY IDS
   LEFT JOIN
    (select person_id::varchar(10)
           ,thirdp_survey_date
           ,coalesce(thirdp_first_choice_bernie,0) as thirdp_first_choice_bernie
           ,coalesce(thirdp_first_choice_trump,0) as thirdp_first_choice_trump
           ,coalesce(thirdp_first_choice_biden,0) as thirdp_first_choice_biden
           ,coalesce(thirdp_first_choice_warren,0) as thirdp_first_choice_warren
           ,coalesce(thirdp_first_choice_buttigieg,0) as thirdp_first_choice_buttigieg
           ,coalesce(thirdp_first_choice_biden_warren_buttigieg,0) as thirdp_first_choice_biden_warren_buttigieg
           ,coalesce(thirdp_first_choice_any,0) as thirdp_first_choice_any
           ,coalesce(thirdp_support_1_id,0) as thirdp_support_1_id
           ,coalesce(thirdp_support_2_id,0) as thirdp_support_2_id
           ,coalesce(thirdp_support_1_2_id,0) as thirdp_support_1_2_id
           ,coalesce(thirdp_support_3_id,0) as thirdp_support_3_id
           ,coalesce(thirdp_support_4_id,0) as thirdp_support_4_id
           ,coalesce(thirdp_support_5_id,0) as thirdp_support_5_id
           ,coalesce(thirdp_support_1_2_3_4_5_id,0) as thirdp_support_1_2_3_4_5_id
           ,case 
           when validtime = 1 THEN 1
           WHEN holdout = 1 THEN 1
           ELSE 0 END AS thirdp_holdout_id
     from 
    (select person_id::varchar(10)
           ,survey_date
           ,state
           ,max(TO_DATE(survey_date, 'YYYY-MM-DD')) OVER (PARTITION BY person_id) AS thirdp_survey_date
           ,ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY TO_DATE(survey_date, 'YYYY-MM-DD') DESC) AS duplicate
           ,case when
            datediff(d, '2020-01-17', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state = 'MN' or
            datediff(d, '2020-01-22', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state IN ('TN','AR','MO','AL','KY','GA','MS') or
            datediff(d, '2020-02-05', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state NOT IN ('MN','TN','AR','MO','AL','KY','GA','MS','NH','NV','IA','SC') then 1 END as validtime -- HARDCODE
           ,case when first_choice = 'Bernie Sanders' then 1 else 0 end as thirdp_first_choice_bernie
           ,case when first_choice = 'Donald Trump' then 1 else 0 end as thirdp_first_choice_trump
           ,case when first_choice = 'Joe Biden' then 1 else 0 end as thirdp_first_choice_biden
           ,case when first_choice = 'Elizabeth Warren' then 1 else 0 end as thirdp_first_choice_warren
           ,case when first_choice = 'Pete Buttigieg' then 1 else 0 end as thirdp_first_choice_buttigieg
           ,case when first_choice IN ('Pete Buttigieg','Elizabeth Warren','Joe Biden') then 1 else 0 end as thirdp_first_choice_biden_warren_buttigieg
           ,case when first_choice IS NOT NULL then 1 else 0 end as thirdp_first_choice_any
           ,case when support_int = 1 then 1 else 0 end as thirdp_support_1_id
           ,case when support_int = 2 then 1 else 0 end as thirdp_support_2_id
           ,case when support_int IN (1,2) then 1 else 0 end as thirdp_support_1_2_id
           ,case when support_int = 3 then 1 else 0 end as thirdp_support_3_id
           ,case when support_int = 4 then 1 else 0 end as thirdp_support_4_id
           ,case when support_int = 5 then 1 else 0 end as thirdp_support_5_id
           ,case when support_int IN (1,2,3,4,5) then 1 else 0 end as thirdp_support_1_2_3_4_5_id
    FROM bernie_data_commons.third_party_ids WHERE legal_for_voter_contact = 't') third
   LEFT JOIN
    (select person_id::varchar, 1 as holdout from haystaq.set_no_current where set_no=3) ho -- HARDCODE
    using(person_id) where person_id is not null and survey_date = thirdp_survey_date and duplicate = 1) thirdp
    using(person_id)
    where ccj_id_1_2_3_4_5 is not null or thirdp_first_choice_any is not null or thirdp_support_1_2_3_4_5_id is not null
);
commit;

------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

-- ActionKit / Mobilize Events: recodes all events to standardized categories related to activism and campaign engagement
begin;
DROP TABLE IF EXISTS bernie_nmarchio2.base_akmobevents;
CREATE TABLE bernie_nmarchio2.base_akmobevents
distkey(ak_event_id) 
sortkey(ak_event_id, mobilize_event_id) as
(SELECT distinct ak_event_id::varchar, mobilize_id::varchar as mobilize_event_id, event_recode from
  (SELECT ak.ak_event_id::varchar(256) ,
          mob.mobilize_id::varchar(256) ,
          CASE
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) ILIKE '%barnstorm%' THEN 'barnstorm'
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) SIMILAR TO ('%canvass%|%bernie-on-the-ballot%|%bernie-journey%|%signature_gathering%') THEN 'canvass'
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) SIMILAR TO ('%solidarityevent%|%solidarity-event%|%solidarity_event%') THEN 'solidarity-action'
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) SIMILAR TO ('%event-bernie-sanders%|%bernie-2020-event%') THEN 'rally-town-hall'
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) SIMILAR TO ('%postcards_for_bernie%|%plan-win-party%|%debate-watch-party%|%debate_watch_party%|%office_opening%|%house-party%|%house_party%') THEN 'small-event'
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) SIMILAR TO ('%call-bernie_virtual%|%phone_bank%|%phonebank%|%phone-bank%') THEN 'phonebank'
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) SIMILAR TO ('%friend-to-friend%|%friend_to_friend%') THEN 'friend-to-friend'
          WHEN lower(coalesce(ak.event_name,mob.event_type_mob)) SIMILAR TO ('%volunteer_training%|%training%') THEN 'training'
          WHEN lower(coalesce(xwalk.ak_title,ak.event_title_ak,mob.event_title_mob)) SIMILAR TO ('%rally%|%town hall%|%panel%|%first friday%|%community meeting%|%community conversation%|%brunch%|%alexandria%|%phillip agnew%|%aoc%|%bernie on the ballot%|%bernie breakfast%|%bernie 2020 discussion%|%nina turner%|%bernie 2020 meets%|%townhall%|%roundtable%') THEN 'rally-town-hall'
          WHEN lower(coalesce(xwalk.ak_title,ak.event_title_ak,mob.event_title_mob)) SIMILAR TO ('%petition%|%canvass%|%petitions%|%volunteering%|%help get bernie%|%bernie journey%') THEN 'canvass'
          WHEN lower(coalesce(xwalk.ak_title,ak.event_title_ak,mob.event_title_mob)) SIMILAR TO ('%phonebank%|%recruitment%|%make calls%') THEN 'phonebank'
          WHEN lower(coalesce(xwalk.ak_title,ak.event_title_ak,mob.event_title_mob)) SIMILAR TO ('%barnstorm%') THEN 'barnstorm'
          WHEN lower(coalesce(xwalk.ak_title,ak.event_title_ak,mob.event_title_mob)) SIMILAR TO ('%training%') THEN 'training'
          WHEN lower(coalesce(xwalk.ak_title,ak.event_title_ak,mob.event_title_mob)) SIMILAR TO ('%parade%|%march%|%teachers for bernie%') THEN 'solidarity-action'
          WHEN lower(coalesce(xwalk.ak_title,ak.event_title_ak,mob.event_title_mob)) SIMILAR TO ('%organizing meeting%') THEN 'solidarity-action'
          WHEN lower(coalesce(ak.event_title_ak,mob.event_title_mob,xwalk.ak_title)) SIMILAR TO ('%delegates%|%meeting%|%talk bernie%|%community discussion%|%meet up%|%team meeting%|%student%|%high school%|% for bernie%|%kickoff meeting%|%leadership meeting%|%kick-off meeting%|%headquarters%|%bernie virtual meeting%|%bash%|%parranda%|%office opening%|%party%|%art hop%|%plan to win%|%debate watch party%|%dinner%|%postcards%|%potluck%|%volunteer appreciation%|%social hour%|%club meeting%|%unidos con bernie%|%debate%|%food drive%|%fiesta%|%happy hour%|%tabling%|%bonfire%') THEN 'small-event'
          ELSE NULL END AS event_recode,
          ROW_NUMBER() OVER(PARTITION BY coalesce(ak.ak_event_id,'0')||coalesce(mob.mobilize_id,'0') ORDER BY event_recode NULLS LAST) AS rownum
   FROM 
    (SELECT ak_event_id ,
            mobilize_id ,
            ak_title ,
            ROW_NUMBER() OVER(PARTITION BY ak_event_id ||'_'||mobilize_id ORDER BY ak_event_id NULLS LAST, mobilize_id NULLS LAST) AS rownum
            FROM core_table_builds.events_xwalk) xwalk
    LEFT JOIN
          (SELECT event.id::int AS ak_event_id,
                  event.title AS event_title_ak ,
                  campaign.name AS event_name
                  FROM ak_bernie.events_event event LEFT JOIN (SELECT id AS campaign_id, name FROM ak_bernie.events_campaign) campaign USING(campaign_id)) ak 
          ON xwalk.ak_event_id = ak.ak_event_id AND xwalk.rownum = 1
    LEFT JOIN
          (SELECT id::varchar(256) AS mobilize_id,
                  lower(event_type::varchar(256)) AS event_type_mob,
                  lower(title::varchar(256)) AS event_title_mob
                  FROM mobilize.events_comprehensive) mob 
          ON mob.mobilize_id = xwalk.mobilize_id AND xwalk.rownum = 1) WHERE rownum = 1 and (ak_event_id is not null or mobilize_id is not null) and event_recode is not null);
commit;

------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

--  Activism Sidetable: all individuals in Bern app, volunteers / attendees / RSVPs in ActionKit and Mobilize, shift volunteers and supporters in MyCampaign, and activism-related Survey Responses
begin;
DROP TABLE IF EXISTS bernie_nmarchio2.base_activists;
CREATE TABLE bernie_nmarchio2.base_activists
distkey(person_id) 
sortkey(person_id) as
(select person_id,
	    voting_address_id,
	    coalesce(myc.all_flags_myc,0) as all_flags_myc, -- all myc flags
	    coalesce(myc.mvp_myc ,0) as mvp_myc ,
        coalesce(myc.activist_myc ,0) as activist_myc ,
        coalesce(myc.canvass_myc ,0) as canvass_myc ,
        coalesce(myc.volunteer_myc ,0) as volunteer_myc ,
        coalesce(myc.constituencies_myc ,0) as constituencies_myc ,
        coalesce(myc.volunteer_shifts_myc ,0) as volunteer_shifts_myc ,
        coalesce(myc.student_myc ,0) as student_myc ,
        coalesce(myc.teacher_myc ,0) as teacher_myc ,
        coalesce(myc.labor_myc,0) as labor_myc,

        coalesce(bern.bernapp,0) as bernapp, -- Bern user
        coalesce(bern.bern_student,0) as bern_student,
        coalesce(bern.bern_union,0) as bern_union,

        coalesce(surveys.all_flags_surveys,0) as all_flags_surveys, -- all survey flags
        coalesce(surveys.sticker_id ,0) as sticker_id ,
        coalesce(surveys.commit2caucus_id ,0) as commit2caucus_id ,
        coalesce(surveys.union_id ,0) as union_id ,
        coalesce(surveys.student_id ,0) as student_id ,
        coalesce(surveys.volunteer_yes_id,0) as volunteer_yes_id,

        coalesce(slack.slack_vol,0) as slack_vol, -- slack vol
        
        coalesce(akmob.akmob_rsvps,0) as akmob_rsvps, -- count of rsvps
        coalesce(akmob.akmob_rsvps_canvass ,0) as akmob_rsvps_canvass ,
        coalesce(akmob.akmob_rsvps_phonebank ,0) as akmob_rsvps_phonebank ,
        coalesce(akmob.akmob_rsvps_small_event ,0) as akmob_rsvps_small_event ,
        coalesce(akmob.akmob_rsvps_friend_to_friend ,0) as akmob_rsvps_friend_to_friend ,
        coalesce(akmob.akmob_rsvps_training ,0) as akmob_rsvps_training ,
        coalesce(akmob.akmob_rsvps_barnstorm ,0) as akmob_rsvps_barnstorm ,
        coalesce(akmob.akmob_rsvps_rally_town_hall ,0) as akmob_rsvps_rally_town_hall ,
        coalesce(akmob.akmob_rsvps_solidarity_action ,0) as akmob_rsvps_solidarity_action ,

        coalesce(akmob.akmob_attended,0) as akmob_attended, -- count of attended
        coalesce(akmob.akmob_attended_canvass ,0) as akmob_attended_canvass ,
        coalesce(akmob.akmob_attended_phonebank ,0) as akmob_attended_phonebank ,
        coalesce(akmob.akmob_attended_small_event ,0) as akmob_attended_small_event ,
        coalesce(akmob.akmob_attended_friend_to_friend ,0) as akmob_attended_friend_to_friend ,
        coalesce(akmob.akmob_attended_training ,0) as akmob_attended_training ,
        coalesce(akmob.akmob_attended_barnstorm ,0) as akmob_attended_barnstorm ,
        coalesce(akmob.akmob_attended_rally_town_hall ,0) as akmob_attended_rally_town_hall ,
        coalesce(akmob.akmob_attended_solidarity_action,0) as akmob_attended_solidarity_action,

        coalesce(bdonor.all_donor_flags,0) as all_donor_flags, -- all donor flags
        coalesce(bdonor.event_signup_0_flag,0) as event_signup_0_flag,
        coalesce(bdonor.event_signup_1_flag,0) as event_signup_1_flag,
        coalesce(bdonor.event_signup_1plus_flag,0) as event_signup_1plus_flag,
        coalesce(bdonor.donor_0_flag,0) as donor_0_flag,
        coalesce(bdonor.donor_1plus_flag,0) as donor_1plus_flag,        
        case 
        when akmob_rsvps >= 2 -- non-binary count
          or akmob_attended > 0 -- non-binary count
          or all_flags_myc = 1
          or all_flags_surveys = 1
          or bernapp = 1
          or slack_vol = 1 then 1
        else 0 end as activist_flag,
        CASE
        WHEN all_flags_myc > 0
            OR bernapp > 0
            OR all_flags_surveys > 0
            OR slack_vol > 0
            OR akmob_rsvps > 0
            OR akmob_attended > 0
            OR all_donor_flags > 0 THEN 1
            ELSE 0
        END AS any_activist_donor_flag

 from 
(SELECT person_id::varchar,
        state_code,
        voting_address_id
      FROM phoenix_analytics.person
      WHERE is_deceased = 'f'
        AND reg_record_merged = 'f'
        AND reg_on_current_file = 't'
        AND reg_voter_flag = 't') p

    left join
    -- MyCampaign Activist Codes
    (select *,
    	CASE WHEN mvp_myc = 1
               OR activist_myc = 1
               OR canvass_myc = 1
               OR volunteer_myc = 1
               OR constituencies_myc = 1
               OR volunteer_shifts_myc = 1
               OR student_myc = 1
               OR teacher_myc = 1
               OR labor_myc = 1 THEN 1
               else 0 end as all_flags_myc
     from
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
     LEFT JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id and ac_lookup.state_code = ac.state_code
     LEFT JOIN bernie_data_commons.master_xwalk_st_myc mx ON mx.myc_van_id = ac.myc_van_id AND mx.state_code = ac.state_code
     WHERE ac.committee_id = 73296 and mx.person_id is not null
     GROUP BY 1)) myc
    using(person_id) 
    
    left join
    -- Bern App
     (SELECT person_id::varchar(10) ,
             count(distinct case when is_student = 't' then person_id end) as bern_student,
             count(distinct case when is_union = 't' then person_id end) as bern_union,
             count(distinct case when bern_canvasser_id is not null or attempted_voter_lookup = 't' then person_id end) as bernapp
             FROM (SELECT * FROM bern_app.canvass_canvasser) canv
             LEFT JOIN
             (SELECT person_id::varchar(10), bern_id, bern_canvasser_id FROM bernie_data_commons.master_xwalk) xwalk 
             ON canv.id = xwalk.bern_canvasser_id where person_id is not null
             GROUP BY 1) bern
     using(person_id)

    left join
    -- Survey Responses
    (select *,
        CASE WHEN sticker_id = 1
               OR commit2caucus_id = 1
               OR union_id = 1
               OR student_id = 1
               OR volunteer_yes_id = 1 THEN 1
        ELSE 0 END AS all_flags_surveys
     from 
    (SELECT person_id::varchar(10) ,
            count(distinct CASE WHEN surveyresponseid IN (40) THEN person_id END) AS sticker_id ,
            count(distinct CASE WHEN surveyresponseid IN (99) THEN person_id END) AS commit2caucus_id ,
            count(distinct CASE WHEN surveyresponseid IN (10) THEN person_id END) AS union_id ,
            count(distinct CASE WHEN surveyresponseid IN (9,103,104) THEN person_id END) AS student_id ,
            count(distinct CASE WHEN surveyresponseid IN (32) THEN person_id END) AS volunteer_yes_id 
            FROM contacts.surveyresponses sr 
            INNER JOIN bernie_data_commons.ccj_dnc ccj using(contactcontact_id) 
            LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid) 
            WHERE person_id is not null group by 1)) surveys
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
    -- Donors
    (select *, 
    	case when event_signup_0_flag = 1
               or event_signup_1_flag = 1
               or event_signup_1plus_flag = 1
               or donor_0_flag = 1
               or donor_1plus_flag  = 1 then 1
               else 0 end as all_donor_flags 
               FROM
    (select distinct person_id, 
                     count(distinct case when n_event_signups = 0 or n_event_signups is null then person_id end) as event_signup_0_flag,
                     count(distinct case when n_event_signups = 1 then person_id end) as event_signup_1_flag,
                     count(distinct case when n_event_signups >= 1 then person_id end) as event_signup_1plus_flag,
                     count(distinct case when n_donations = 0 then person_id end) as donor_0_flag,
                     count(distinct case when n_donations >= 1 then person_id end) as donor_1plus_flag
   	from bernie_jshuman.donor_basetable where person_id is not null group by 1)) bdonor
    using(person_id)

    left join
    -- ActionKit / Mobilize
    (SELECT person_id::varchar,
            count(person_id) as akmob_rsvps,
            count(CASE WHEN event_recode = 'canvass' THEN person_id END) AS akmob_rsvps_canvass ,
            count(CASE WHEN event_recode = 'phonebank' THEN person_id END) AS akmob_rsvps_phonebank ,
            count(CASE WHEN event_recode = 'small-event' THEN person_id END) AS akmob_rsvps_small_event ,
            count(CASE WHEN event_recode = 'friend-to-friend' THEN person_id END) AS akmob_rsvps_friend_to_friend ,
            count(CASE WHEN event_recode = 'training' THEN person_id END) AS akmob_rsvps_training ,
            count(CASE WHEN event_recode = 'barnstorm' THEN person_id END) AS akmob_rsvps_barnstorm ,
            count(CASE WHEN event_recode = 'rally-town-hall' THEN person_id END) AS akmob_rsvps_rally_town_hall ,
            count(CASE WHEN event_recode = 'solidarity-action' THEN person_id END) AS akmob_rsvps_solidarity_action ,
            count(case when attended = 't' then person_id end) as akmob_attended,
            count(CASE WHEN event_recode = 'canvass' and attended = 't' THEN person_id END) AS akmob_attended_canvass ,
            count(CASE WHEN event_recode = 'phonebank' and attended = 't' THEN person_id END) AS akmob_attended_phonebank ,
            count(CASE WHEN event_recode = 'small-event' and attended = 't' THEN person_id END) AS akmob_attended_small_event ,
            count(CASE WHEN event_recode = 'friend-to-friend' and attended = 't' THEN person_id END) AS akmob_attended_friend_to_friend ,
            count(CASE WHEN event_recode = 'training' and attended = 't' THEN person_id END) AS akmob_attended_training ,
            count(CASE WHEN event_recode = 'barnstorm' and attended = 't' THEN person_id END) AS akmob_attended_barnstorm ,
            count(CASE WHEN event_recode = 'rally-town-hall' and attended = 't' THEN person_id END) AS akmob_attended_rally_town_hall ,
            count(CASE WHEN event_recode = 'solidarity-action' and attended = 't' THEN person_id END) AS akmob_attended_solidarity_action 
           from (
     /* -- comment off the Mobilize data since it pulls in only 75 IDs -- we have to add Mobilize to a person match pipeline to add them to the xwalk
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
      (select * from bernie_nmarchio2.base_akmobevents where mobilize_event_id is not null) using(mobilize_event_id)) mob
    union all
    */
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
      (select * from bernie_nmarchio2.base_akmobevents where ak_event_id is not null) using(ak_event_id))
               ) where person_id is not null and event_recode is not null group by 1) akmob
    using(person_id) where any_activist_donor_flag > 0); -- change
commit;

--  Activism Sidetable Unmatched: supplemental to the above covering only unmatched persons without person_id primary keys
begin;
DROP TABLE IF EXISTS bernie_nmarchio2.base_activists_unmatched;
CREATE TABLE bernie_nmarchio2.base_activists_unmatched
distkey(jsonid_encoded) 
sortkey(jsonid_encoded) as
(select xwalk.person_id,
        --NULL as voting_address_id,
        xwalk.jsonid_encoded,
        xwalk.myc_van_id,
        xwalk.actionkit_id,
        xwalk.bern_canvasser_id,        
        xwalk.state_code,
        case when xwalk.student_hash is not null then 1 else 0 end as student_hash_flag,
    
        ccj.ccj_contactdate,
        case when datediff(d, TO_DATE(ccj.ccj_contactdate, 'YYYY-MM-DD'), CURRENT_DATE) <= 30 then 1 else 0 end as ccj_contacted_last_30_days,
        ccj.ccj_contact_made,
        ccj.ccj_negative_result,
        ccj.ccj_id_1,
        ccj.ccj_id_1_2,
        ccj.ccj_id_2,
        ccj.ccj_id_3,
        ccj.ccj_id_4,
        ccj.ccj_id_5,
        ccj.ccj_id_1_2_3_4_5,
        CASE
        WHEN ccj.ccj_id_1_2_3_4_5 = 0 or ccj.ccj_id_1_2_3_4_5 IS NULL THEN 'Uncontacted'
        WHEN ccj.ccj_holdout_id = 1 THEN 'Holdout'
        WHEN ccj.ccj_holdout_id = 0 THEN 'Training' END AS ccj_holdout_group,

        coalesce(myc.all_flags_myc,0) as all_flags_myc, -- all myc flags
        
        coalesce(myc.mvp_myc ,0) as mvp_myc ,
        coalesce(myc.activist_myc ,0) as activist_myc ,
        coalesce(myc.canvass_myc ,0) as canvass_myc ,
        coalesce(myc.volunteer_myc ,0) as volunteer_myc ,
        coalesce(myc.constituencies_myc ,0) as constituencies_myc ,
        coalesce(myc.volunteer_shifts_myc ,0) as volunteer_shifts_myc ,
        coalesce(myc.student_myc ,0) as student_myc ,
        coalesce(myc.teacher_myc ,0) as teacher_myc ,
        coalesce(myc.labor_myc,0) as labor_myc,
        
        coalesce(surveys.all_flags_surveys,0) as all_flags_surveys, -- all survey flags
        
        coalesce(surveys.sticker_id ,0) as sticker_id ,
        coalesce(surveys.commit2caucus_id ,0) as commit2caucus_id ,
        coalesce(surveys.union_id ,0) as union_id ,
        coalesce(surveys.student_id ,0) as student_id ,
        coalesce(surveys.volunteer_yes_id,0) as volunteer_yes_id,
        

        coalesce(bern.bernapp,0) as bernapp, -- Bern user
        coalesce(bern.bern_student,0) as bern_student,
        coalesce(bern.bern_union,0) as bern_union,
        coalesce(slack.slack_vol,0) as slack_vol, -- slack vol
        
        coalesce(akmob.akmob_rsvps,0) as akmob_rsvps, -- count of rsvps
        
        coalesce(akmob.akmob_rsvps_canvass ,0) as akmob_rsvps_canvass ,
        coalesce(akmob.akmob_rsvps_phonebank ,0) as akmob_rsvps_phonebank ,
        coalesce(akmob.akmob_rsvps_small_event ,0) as akmob_rsvps_small_event ,
        coalesce(akmob.akmob_rsvps_friend_to_friend ,0) as akmob_rsvps_friend_to_friend ,
        coalesce(akmob.akmob_rsvps_training ,0) as akmob_rsvps_training ,
        coalesce(akmob.akmob_rsvps_barnstorm ,0) as akmob_rsvps_barnstorm ,
        coalesce(akmob.akmob_rsvps_rally_town_hall ,0) as akmob_rsvps_rally_town_hall ,
        coalesce(akmob.akmob_rsvps_solidarity_action ,0) as akmob_rsvps_solidarity_action ,
        

        coalesce(akmob.akmob_attended,0) as akmob_attended, -- count of attended
        
        coalesce(akmob.akmob_attended_canvass ,0) as akmob_attended_canvass ,
        coalesce(akmob.akmob_attended_phonebank ,0) as akmob_attended_phonebank ,
        coalesce(akmob.akmob_attended_small_event ,0) as akmob_attended_small_event ,
        coalesce(akmob.akmob_attended_friend_to_friend ,0) as akmob_attended_friend_to_friend ,
        coalesce(akmob.akmob_attended_training ,0) as akmob_attended_training ,
        coalesce(akmob.akmob_attended_barnstorm ,0) as akmob_attended_barnstorm ,
        coalesce(akmob.akmob_attended_rally_town_hall ,0) as akmob_attended_rally_town_hall ,
        coalesce(akmob.akmob_attended_solidarity_action,0) as akmob_attended_solidarity_action,
        

        coalesce(bdonor.all_donor_flags,0) as all_donor_flags, -- all donor flags
        
        coalesce(bdonor.event_signup_0_flag,0) as event_signup_0_flag,
        coalesce(bdonor.event_signup_1_flag,0) as event_signup_1_flag,
        coalesce(bdonor.event_signup_1plus_flag,0) as event_signup_1plus_flag,
        coalesce(bdonor.donor_0_flag,0) as donor_0_flag,
        
        coalesce(bdonor.donor_1plus_flag,0) as donor_1plus_flag,  

        case 
        when akmob_rsvps >= 2 -- non-binary count
          or akmob_attended > 0 -- non-binary count
          or all_flags_myc = 1
          or all_flags_surveys = 1
          or bernapp = 1
          or slack_vol = 1 then 1
        else 0 end as activist_flag,
        NULL as activist_household_flag,

        CASE
        WHEN all_flags_myc > 0
            OR bernapp > 0
            OR all_flags_surveys > 0
            OR slack_vol > 0
            OR akmob_rsvps > 0
            OR akmob_attended > 0
            OR all_donor_flags > 0 THEN 1
            ELSE 0
        END AS any_activist_donor_flag,

        '1 - Target universe'  as electorate_2way,
        '2 - Must Register as Dem' as vote_ready_5way,
        '0 - Donors, Activists, Supporters' as support_guardrail,
        '0 - Donors and Activists' as support_guardrail_validation,
        '0 - Donors, Activists, Supporters' as support_guardrail_extra,
        '0 - Donors and Activists' as support_guardrail_extra_validation

 from 
    (SELECT person_id,
            state_code,
            myc_van_id,
            actionkit_id,
            bern_canvasser_id,
            jsonid_encoded,
            email,
            student_hash
      FROM bernie_data_commons.master_xwalk) xwalk
    left join

    (SELECT person_id::varchar,
            state_code,
            voting_address_id
      FROM phoenix_analytics.person
      WHERE is_deceased = 'f'
        AND reg_record_merged = 'f'
        AND reg_on_current_file = 't'
        AND reg_voter_flag = 't' ) p
    on p.person_id = xwalk.person_id

    left join
    -- CCJ Field IDs
    (select jsonid_encoded
            ,ccj_contactdate
            ,coalesce(ccj_contact_made,0) as ccj_contact_made
            ,coalesce(ccj_negative_result,0) as ccj_negative_result
            ,coalesce(ccj_id_1,0) as ccj_id_1
            ,coalesce(ccj_id_1_2,0) as ccj_id_1_2
            ,coalesce(ccj_id_2,0) as ccj_id_2
            ,coalesce(ccj_id_3,0) as ccj_id_3
            ,coalesce(ccj_id_4,0) as ccj_id_4
            ,coalesce(ccj_id_5,0) as ccj_id_5
            ,coalesce(ccj_id_1_2_3_4_5,0) as ccj_id_1_2_3_4_5
            ,case 
            WHEN validtime = 1 then 1
            ELSE 0 END AS ccj_holdout_id
    FROM 
    (SELECT jsonid_encoded
            ,contactdate
            ,max(TO_DATE(contactdate, 'YYYY-MM-DD')) OVER (PARTITION BY person_id) AS ccj_contactdate
            ,case when
             datediff(d, '2020-01-17', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state = 'MN' or
             datediff(d, '2020-01-22', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state IN ('TN','AR','MO','AL','KY','GA','MS') or
             datediff(d, '2020-02-05', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state NOT IN ('MN','TN','AR','MO','AL','KY','GA','MS','NH','NV','IA','SC') THEN 1 END as validtime -- HARDCODE
            ,CASE WHEN resultcode IN ('Canvassed', 'Do Not Contact', 'Refused', 'Call Back', 'Language Barrier', 'Hostile', 'Come Back', 'Cultivation', 'Refused Contact', 'Spanish', 'Other', 'Not Interested') THEN 1 ELSE 0 END AS ccj_contact_made 
            ,CASE WHEN resultcode IN ('Do Not Contact','Hostile','Refused','Refused Contact') OR (support_int = 4 OR support_int = 5) THEN 1 ELSE 0 END AS ccj_negative_result 
            ,CASE WHEN support_int = 1 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1 
            ,CASE WHEN support_int IN (1,2) AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1_2 
            ,CASE WHEN support_int = 2 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_2 
            ,CASE WHEN support_int = 3 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_3 
            ,CASE WHEN support_int = 4 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_4 
            ,CASE WHEN support_int = 5 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_5 
            ,CASE WHEN support_int IN (1,2,3,4,5) AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1_2_3_4_5
            FROM bernie_data_commons.contactcontacts_joined
            WHERE unique_id_flag = 1 AND person_id IS NULL) where jsonid_encoded is not null and ccj_contactdate = contactdate) ccj
    using(jsonid_encoded)

    left join
    -- MyCampaign Activist Codes
    (select *,
    	CASE WHEN mvp_myc = 1
               OR activist_myc = 1
               OR canvass_myc = 1
               OR volunteer_myc = 1
               OR constituencies_myc = 1
               OR volunteer_shifts_myc = 1
               OR student_myc = 1
               OR teacher_myc = 1
               OR labor_myc = 1 THEN 1
               else 0 end as all_flags_myc
     from
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
     LEFT JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id and ac_lookup.state_code = ac.state_code
     WHERE ac.committee_id = 73296 and ac.myc_van_id is not null
     GROUP BY 1,2) ) myc
    ON xwalk.myc_van_id = myc.myc_van_id AND xwalk.state_code = myc.state_code
    
    left join
    -- Bern App
    (SELECT bern_canvasser_id,
    	    count(distinct case when is_student = 't' then bern_canvasser_id end) as bern_student,
            count(distinct case when is_union = 't' then bern_canvasser_id end) as bern_union,
            count(distinct case when bern_canvasser_id is not null or attempted_voter_lookup = 't' then bern_canvasser_id end) as bernapp
            FROM (SELECT id as bern_canvasser_id, attempted_voter_lookup, is_student , is_union FROM bern_app.canvass_canvasser) canv
            where bern_canvasser_id is not null
            GROUP BY 1) bern
            using(bern_canvasser_id) 

    left join
    -- Survey Responses
    (select *,
        CASE WHEN sticker_id = 1
               OR commit2caucus_id = 1
               OR union_id = 1
               OR student_id = 1
               OR volunteer_yes_id = 1 THEN 1
        ELSE 0 END AS all_flags_surveys
     from 
    (SELECT jsonid_encoded,
            count(distinct CASE WHEN surveyresponseid IN (40) THEN jsonid_encoded END) AS sticker_id ,
            count(distinct CASE WHEN surveyresponseid IN (99) THEN jsonid_encoded END) AS commit2caucus_id ,
            count(distinct CASE WHEN surveyresponseid IN (10) THEN jsonid_encoded END) AS union_id ,
            count(distinct CASE WHEN surveyresponseid IN (9,103,104) THEN jsonid_encoded END) AS student_id ,
            count(distinct CASE WHEN surveyresponseid IN (32) THEN jsonid_encoded END) AS volunteer_yes_id 
            FROM contacts.surveyresponses sr 
            INNER JOIN bernie_data_commons.contactcontacts_joined ccj using(contactcontact_id) 
            LEFT JOIN contacts.surveyresponsetext srt using(surveyresponseid) 
            WHERE jsonid_encoded is not null and person_id is null group by 1) ) surveys
    using(jsonid_encoded) 

    left join
    -- Slack
    (SELECT profile_email as email,
            count(distinct CASE WHEN deleted = 'f' THEN profile_email END) AS slack_vol
            from 
            (SELECT id AS slack_id, name, deleted, profile_email FROM slack.vol_users) slack
            LEFT JOIN
            (SELECT person_id::varchar, email, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY email NULLS LAST) AS rownum FROM bernie_data_commons.master_xwalk) xwalk_master 
            ON xwalk_master.email = slack.profile_email AND xwalk_master.rownum = 1
            where person_id is not null
            GROUP BY 1) slack
    USING(email)
    
    left join
    -- Donors
    (select *, 
    	case when event_signup_0_flag = 1
               or event_signup_1_flag = 1
               or event_signup_1plus_flag = 1
               or donor_0_flag = 1
               or donor_1plus_flag  = 1 then 1
               else 0 end as all_donor_flags 
               FROM
    (select distinct user_id as actionkit_id, 
                     count(distinct case when n_event_signups = 0 or n_event_signups is null then actionkit_id end) as event_signup_0_flag,
                     count(distinct case when n_event_signups = 1 then actionkit_id end) as event_signup_1_flag,
                     count(distinct case when n_event_signups > 1 then actionkit_id end) as event_signup_1plus_flag,
                     count(distinct case when n_donations = 0 then actionkit_id end) as donor_0_flag,
                     count(distinct case when n_donations > 0 then actionkit_id end) as donor_1plus_flag
   	from bernie_jshuman.donor_basetable where user_id is not null and actionkit_id is null group by 1)) bdonor
    using(actionkit_id)

    left join
    -- ActionKit / Mobilize
    (SELECT actionkit_id::bigint,
            count(actionkit_id) as akmob_rsvps,
            count(CASE WHEN event_recode = 'canvass' THEN actionkit_id END) AS akmob_rsvps_canvass ,
            count(CASE WHEN event_recode = 'phonebank' THEN actionkit_id END) AS akmob_rsvps_phonebank ,
            count(CASE WHEN event_recode = 'small-event' THEN actionkit_id END) AS akmob_rsvps_small_event ,
            count(CASE WHEN event_recode = 'friend-to-friend' THEN actionkit_id END) AS akmob_rsvps_friend_to_friend ,
            count(CASE WHEN event_recode = 'training' THEN actionkit_id END) AS akmob_rsvps_training ,
            count(CASE WHEN event_recode = 'barnstorm' THEN actionkit_id END) AS akmob_rsvps_barnstorm ,
            count(CASE WHEN event_recode = 'rally-town-hall' THEN actionkit_id END) AS akmob_rsvps_rally_town_hall ,
            count(CASE WHEN event_recode = 'solidarity-action' THEN actionkit_id END) AS akmob_rsvps_solidarity_action ,
            count(case when attended = 't' then actionkit_id end) as akmob_attended,
            count(CASE WHEN event_recode = 'canvass' and attended = 't' THEN actionkit_id END) AS akmob_attended_canvass ,
            count(CASE WHEN event_recode = 'phonebank' and attended = 't' THEN actionkit_id END) AS akmob_attended_phonebank ,
            count(CASE WHEN event_recode = 'small-event' and attended = 't' THEN actionkit_id END) AS akmob_attended_small_event ,
            count(CASE WHEN event_recode = 'friend-to-friend' and attended = 't' THEN actionkit_id END) AS akmob_attended_friend_to_friend ,
            count(CASE WHEN event_recode = 'training' and attended = 't' THEN actionkit_id END) AS akmob_attended_training ,
            count(CASE WHEN event_recode = 'barnstorm' and attended = 't' THEN actionkit_id END) AS akmob_attended_barnstorm ,
            count(CASE WHEN event_recode = 'rally-town-hall' and attended = 't' THEN actionkit_id END) AS akmob_attended_rally_town_hall ,
            count(CASE WHEN event_recode = 'solidarity-action' and attended = 't' THEN actionkit_id END) AS akmob_attended_solidarity_action 
           from (
    select distinct actionkit_id, ak_event_id, event_recode, attended from -- actionkit_id, ak_event_id,
      (SELECT DISTINCT user_id::varchar(256) AS actionkit_id ,
                       event_id::varchar(256) AS ak_event_id ,
                       attended
       FROM ak_bernie.events_eventsignup) 
       JOIN bernie_nmarchio2.base_akmobevents using(ak_event_id)) 
    where event_recode is not null and actionkit_id is not null
    group by 1) akmob
    using(actionkit_id) where any_activist_donor_flag > 0 and xwalk.person_id is null); 
commit;

------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

-- Households of Activists, Donors, and Field / Third Party IDs: Data from the above tables aggregated to the household level
begin;
DROP TABLE IF EXISTS bernie_nmarchio2.base_household;
CREATE TABLE bernie_nmarchio2.base_household
distkey(voting_address_id) 
sortkey(voting_address_id) as
(select voting_address_id::varchar,

        coalesce(bactive.any_activist_donor_household_flag,0) as any_activist_donor_household_flag,
        coalesce(bactive.activist_household_flag,0) as activist_household_flag,
        coalesce(bactive.all_flags_myc_household_flag,0) as all_flags_myc_household_flag,
        coalesce(bactive.all_flags_surveys_household_flag,0) as all_flags_surveys_household_flag,
        coalesce(bactive.donor_1plus_household_flag,0) as donor_1plus_household_flag,        
        coalesce(bactive.bernapp_household_flag,0) as bernapp_household_flag,
        coalesce(bactive.slack_vol_household_flag,0) as slack_vol_household_flag,
        coalesce(bactive.akmob_rsvps_household_flag,0) as akmob_rsvps_household_flag,
        coalesce(bactive.akmob_attended_household_flag,0) as akmob_attended_household_flag,

        coalesce(bvalid.ccj_id_1_hh,0) as ccj_id_1_hh,
        coalesce(bvalid.ccj_id_2_hh,0) as ccj_id_2_hh,
        coalesce(bvalid.ccj_id_3_hh,0) as ccj_id_3_hh,
        coalesce(bvalid.ccj_id_4_hh,0) as ccj_id_4_hh,
        coalesce(bvalid.ccj_id_5_hh,0) as ccj_id_5_hh,
        coalesce(bvalid.ccj_id_1_2_3_4_5_hh,0) as ccj_id_1_2_3_4_5_hh,
        coalesce(bvalid.thirdp_first_choice_bernie_hh,0) as thirdp_first_choice_bernie_hh,
        coalesce(bvalid.thirdp_first_choice_trump_hh,0) as thirdp_first_choice_trump_hh,
        coalesce(bvalid.thirdp_first_choice_biden_warren_buttigieg_hh,0) as thirdp_first_choice_biden_warren_buttigieg_hh,
        coalesce(bvalid.thirdp_first_choice_any_hh,0) as thirdp_first_choice_any_hh,
        coalesce(bvalid.thirdp_support_1_id_hh,0) as thirdp_support_1_id_hh,
        coalesce(bvalid.thirdp_support_2_id_hh,0) as thirdp_support_2_id_hh,
        coalesce(bvalid.thirdp_support_3_id_hh,0) as thirdp_support_3_id_hh,
        coalesce(bvalid.thirdp_support_4_id_hh,0) as thirdp_support_4_id_hh,
        coalesce(bvalid.thirdp_support_5_id_hh,0) as thirdp_support_5_id_hh,
        coalesce(bvalid.thirdp_support_1_2_3_4_5_id_hh,0) as thirdp_support_1_2_3_4_5_id_hh

from 
(select voting_address_id::varchar,
	    count(distinct case when any_activist_donor_flag = 1 then voting_address_id end) as any_activist_donor_household_flag,
        count(distinct CASE WHEN activist_flag = 1 then voting_address_id end) as activist_household_flag,
        count(distinct case when all_flags_myc = 1 then voting_address_id end) as all_flags_myc_household_flag,
        count(distinct case when all_flags_surveys = 1 then voting_address_id end) as all_flags_surveys_household_flag,
        count(distinct case when donor_1plus_flag = 1 then voting_address_id end) as donor_1plus_household_flag,        
        count(distinct case when bernapp = 1 then voting_address_id end) as bernapp_household_flag,
        count(distinct case when slack_vol = 1 then voting_address_id end) as slack_vol_household_flag,
        count(distinct CASE WHEN akmob_rsvps >= 2 then voting_address_id end) as akmob_rsvps_household_flag,
        count(distinct CASE WHEN akmob_attended > 0 then voting_address_id end) as akmob_attended_household_flag
from bernie_nmarchio2.base_activists group by 1) bactive
full join
(select voting_address_id::varchar, 
        count(distinct CASE WHEN ccj_id_1 = 1 then voting_address_id end) as ccj_id_1_hh,
        count(distinct CASE WHEN ccj_id_2 = 1 then voting_address_id end) as ccj_id_2_hh,
        count(distinct CASE WHEN ccj_id_3 = 1 then voting_address_id end) as ccj_id_3_hh,
        count(distinct CASE WHEN ccj_id_4 = 1 then voting_address_id end) as ccj_id_4_hh,
        count(distinct CASE WHEN ccj_id_5 = 1 then voting_address_id end) as ccj_id_5_hh,
        count(distinct CASE WHEN ccj_id_1_2_3_4_5 = 1 then voting_address_id end) as ccj_id_1_2_3_4_5_hh,
        count(distinct CASE WHEN thirdp_first_choice_bernie = 1 then voting_address_id end) as thirdp_first_choice_bernie_hh,
        count(distinct CASE WHEN thirdp_first_choice_trump = 1 then voting_address_id end) as thirdp_first_choice_trump_hh,
        count(distinct CASE WHEN thirdp_first_choice_biden_warren_buttigieg = 1 then voting_address_id end) as thirdp_first_choice_biden_warren_buttigieg_hh,
        count(distinct CASE WHEN thirdp_first_choice_any = 1 then voting_address_id end) as thirdp_first_choice_any_hh,
        count(distinct CASE WHEN thirdp_support_1_id = 1 then voting_address_id end) as thirdp_support_1_id_hh,
        count(distinct CASE WHEN thirdp_support_2_id = 1 then voting_address_id end) as thirdp_support_2_id_hh,
        count(distinct CASE WHEN thirdp_support_3_id = 1 then voting_address_id end) as thirdp_support_3_id_hh,
        count(distinct CASE WHEN thirdp_support_4_id = 1 then voting_address_id end) as thirdp_support_4_id_hh,
        count(distinct CASE WHEN thirdp_support_5_id = 1 then voting_address_id end) as thirdp_support_5_id_hh,
        count(distinct CASE WHEN thirdp_support_1_2_3_4_5_id = 1 then voting_address_id end) as thirdp_support_1_2_3_4_5_id_hh
from phoenix_analytics.person inner join bernie_nmarchio2.base_validation using(person_id) group by 1) bvalid
using(voting_address_id) where voting_address_id is not null);
commit;

------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------

-- Base Universe Main Table: Main Universes table build query
begin;

set query_group to 'importers';
set wlm_query_slot_count to 3;

DROP TABLE IF EXISTS bernie_data_commons.base_universe;
CREATE TABLE bernie_data_commons.base_universe
distkey(person_id) 
sortkey(person_id) AS
  (SELECT p.person_id::varchar,
  	  p.voting_address_id,
  	  xwalk.jsonid_encoded,
          xwalk.myc_van_id,
          xwalk.st_myc_van_id,
          xwalk.myv_van_id,
          xwalk.st_myv_van_id,
  	  xwalk.actionkit_id, 
          xwalk.bern_canvasser_id,
          p.state_code,

  	      -- Geographic codes
          p.county_fips,
          p.county_name,
          p.dnc_precinct_id,
          p.van_precinct_id,
          p.voting_address_latitude,
          p.voting_address_longitude,

          -- Party
          voterinfo.party_8way,
          voterinfo.party_3way,
          voterinfo.civis_2020_partisanship,

          -- Vote information
          voterinfo.vote_history_6way,
          voterinfo.early_vote_history_3way,
          voterinfo.early_voting_3way,
          voterinfo.registered_in_state_3way,
          voterinfo.absentee_voting_4way,
          voterinfo.dem_primary_eligible_2way,

          -- Demographics
          voterinfo.race_5way,
          voterinfo.spanish_language_2way,
          voterinfo.age_5way,
          voterinfo.ideology_5way,
          voterinfo.education_2way,
          voterinfo.income_5way,
          voterinfo.gender_2way,
          voterinfo.urban_3way,
          voterinfo.child_in_hh_2way,
          voterinfo.marital_2way,
          voterinfo.religion_9way,
          nct.flag_muslim,
          nct.flag_student_age,
          nct.flag_union,
          nct.flag_veteran,
          case when xwalk.student_hash is not null then 1 
          else 0 end as student_hash_flag,
          case when xwalk.student_hash is not null or 
          bactive.student_myc = 1 or
          bactive.student_id = 1 or 
          bactive.bern_student = 1 then 1
          else 0 end as student_flag, 

          -- Support scores
          bdcas.donut_segment,
          bdcas.current_support_raw,
          bdcas.current_support_raw_100,
          bdcas.field_id_1_score,
          bdcas.field_id_1_score_100,
          bdcas.field_id_composite_score,
          bdcas.field_id_composite_score_100,
          bdcas.field_id_5_score,
          bdcas.field_id_5_score_100,
          bdcas.turnout_current,
          bdcas.turnout_current_100,
          bdcas.sanders_strong_support_score,
          bdcas.sanders_strong_support_score_100,
          bdcas.sanders_very_excited_score,
          bdcas.sanders_very_excited_score_100,
          bdcas.biden_support,
          bdcas.biden_support_100,
          bdcas.warren_support,
          bdcas.warren_support_100,
          bdcas.buttigieg_support,
          bdcas.buttigieg_support_100,
          bdcas.bloomberg_support,
          bdcas.bloomberg_support_100,
          bdcas.klobuchar_support,
          bdcas.klobuchar_support_100,
          bdcas.steyer_support,
          bdcas.steyer_support_100,
          bdcas.trump_support,
          bdcas.trump_support_100,
          round((bdcas.current_support_raw * bdcas.turnout_current),4) AS bernie_net_votes_current,
 
          -- Message microtargeting
          voterinfo.civis_2018_one_pct_persuasion,
          voterinfo.civis_2018_marijuana_persuasion,
          voterinfo.civis_2018_college_persuasion,
          voterinfo.civis_2018_welcome_persuasion,
          voterinfo.civis_2018_sexual_assault_persuasion,
          voterinfo.civis_2018_economic_persuasion,

          -- Spoke model
          spokemodel.spoke_support_1box,
          spokemodel.spoke_persuasion_1plus,
          spokemodel.spoke_persuasion_1minus,
          spokemodel.spoke_support_1box_100,
          spokemodel.spoke_persuasion_1plus_100,
          spokemodel.spoke_persuasion_1minus_100 ,

          -- Vol model
          volmodel.attendee,
          volmodel.kickoff_party_rally_barnstorm_attendee,
          volmodel.canvasser_phonebank_attendee,
          volmodel.bernie_action,
          volmodel.attendee_100,
          volmodel.kickoff_party_rally_barnstorm_attendee_100,
          volmodel.canvasser_phonebank_attendee_100,
          volmodel.bernie_action_100,

          -- Third Party IDs
          bvalidate.thirdp_survey_date,
          case when datediff(d, TO_DATE(bvalidate.thirdp_survey_date, 'YYYY-MM-DD'), CURRENT_DATE) <= 30 then 1 else 0 end as thirdp_contacted_last_30_days,
          bvalidate.thirdp_first_choice_bernie,
          bvalidate.thirdp_first_choice_trump,
          bvalidate.thirdp_first_choice_biden,
          bvalidate.thirdp_first_choice_warren,
          bvalidate.thirdp_first_choice_buttigieg,
          bvalidate.thirdp_first_choice_biden_warren_buttigieg,
          bvalidate.thirdp_first_choice_any,
          bvalidate.thirdp_support_1_id,
          bvalidate.thirdp_support_2_id,
          bvalidate.thirdp_support_1_2_id,
          bvalidate.thirdp_support_3_id,
          bvalidate.thirdp_support_4_id,
          bvalidate.thirdp_support_5_id,
          bvalidate.thirdp_support_1_2_3_4_5_id,
          CASE
          WHEN bvalidate.thirdp_support_1_2_3_4_5_id = 0 or bvalidate.thirdp_support_1_2_3_4_5_id IS NULL THEN 'Uncontacted'
          WHEN bvalidate.thirdp_first_choice_any = 0 or bvalidate.thirdp_first_choice_any IS NULL then 'Uncontacted'
          WHEN bvalidate.thirdp_holdout_id = 1 THEN 'Holdout'
          WHEN bvalidate.thirdp_holdout_id = 0 THEN 'Training' END AS thirdp_holdout_group,

          -- Third Party IDs Household
          case when bhousehold.thirdp_first_choice_bernie_hh = 1 and bvalidate.thirdp_first_choice_bernie <> 1 then 1 else 0 end as thirdp_first_choice_bernie_hh,
          case when bhousehold.thirdp_first_choice_trump_hh = 1 and bvalidate.thirdp_first_choice_trump <> 1 then 1 else 0 end as thirdp_first_choice_trump_hh,
          case when bhousehold.thirdp_first_choice_biden_warren_buttigieg_hh = 1 and bvalidate.thirdp_first_choice_biden_warren_buttigieg <> 1 then 1 else 0 end as thirdp_first_choice_biden_warren_buttigieg_hh,
          case when bhousehold.thirdp_first_choice_any_hh = 1 and bvalidate.thirdp_first_choice_any <> 1 then 1 else 0 end as thirdp_first_choice_any_hh,
          case when bhousehold.thirdp_support_1_id_hh = 1 and bvalidate.thirdp_support_1_id <> 1 then 1 else 0 end as thirdp_support_1_id_hh,
          case when bhousehold.thirdp_support_2_id_hh = 1 and bvalidate.thirdp_support_2_id <> 1 then 1 else 0 end as thirdp_support_2_id_hh,
          case when bhousehold.thirdp_support_3_id_hh = 1 and bvalidate.thirdp_support_3_id <> 1 then 1 else 0 end as thirdp_support_3_id_hh,
          case when bhousehold.thirdp_support_4_id_hh = 1 and bvalidate.thirdp_support_4_id <> 1 then 1 else 0 end as thirdp_support_4_id_hh,
          case when bhousehold.thirdp_support_5_id_hh = 1 and bvalidate.thirdp_support_5_id <> 1 then 1 else 0 end as thirdp_support_5_id_hh,
          case when bhousehold.thirdp_support_1_2_3_4_5_id_hh = 1 and bvalidate.thirdp_support_1_2_3_4_5_id <> 1 then 1 else 0 end as thirdp_support_1_2_3_4_5_id_hh,

          -- Field IDs
          bvalidate.ccj_contactdate,
          case when datediff(d, TO_DATE(bvalidate.ccj_contactdate, 'YYYY-MM-DD'), CURRENT_DATE) <= 30 then 1 else 0 end as ccj_contacted_last_30_days,
          bvalidate.ccj_contact_made,
          bvalidate.ccj_negative_result,
          bvalidate.ccj_id_1,
          bvalidate.ccj_id_1_2,
          bvalidate.ccj_id_2,
          bvalidate.ccj_id_3,
          bvalidate.ccj_id_4,
          bvalidate.ccj_id_5,
          bvalidate.ccj_id_1_2_3_4_5,
          CASE
          WHEN bvalidate.ccj_id_1_2_3_4_5 = 0 or bvalidate.ccj_id_1_2_3_4_5 IS NULL THEN 'Uncontacted'
          WHEN bvalidate.ccj_holdout_id = 1 THEN 'Holdout'
          WHEN bvalidate.ccj_holdout_id = 0 THEN 'Training' END AS ccj_holdout_group,

		  -- Field IDs Household
          case when bhousehold.ccj_id_1_hh = 1 and bvalidate.ccj_id_1 <> 1 then 1 else 0 end as ccj_id_1_hh,
          case when bhousehold.ccj_id_2_hh = 1 and bvalidate.ccj_id_2 <> 1 then 1 else 0 end as ccj_id_2_hh,
          case when bhousehold.ccj_id_3_hh = 1 and bvalidate.ccj_id_3 <> 1 then 1 else 0 end as ccj_id_3_hh,
          case when bhousehold.ccj_id_4_hh = 1 and bvalidate.ccj_id_4 <> 1 then 1 else 0 end as ccj_id_4_hh,
          case when bhousehold.ccj_id_5_hh = 1 and bvalidate.ccj_id_5 <> 1 then 1 else 0 end as ccj_id_5_hh,
          case when bhousehold.ccj_id_1_2_3_4_5_hh = 1 and bvalidate.ccj_id_1_2_3_4_5 <> 1 then 1 else 0 end as ccj_id_1_2_3_4_5_hh,

          -- Activism / Donor Flags
          -- MyCampaign
          coalesce(bactive.all_flags_myc, 0) as all_flags_myc, -- all MyCampaign flags
                  coalesce(bactive.mvp_myc, 0) as mvp_myc,
                  coalesce(bactive.activist_myc, 0) as activist_myc,
                  coalesce(bactive.canvass_myc, 0) as canvass_myc,
                  coalesce(bactive.volunteer_myc, 0) as volunteer_myc,
                  coalesce(bactive.constituencies_myc, 0) as constituencies_myc,
                  coalesce(bactive.volunteer_shifts_myc, 0) as volunteer_shifts_myc,
                  coalesce(bactive.student_myc, 0) as student_myc,
                  coalesce(bactive.teacher_myc, 0) as teacher_myc,
                  coalesce(bactive.labor_myc, 0) as labor_myc,
          
          -- Surveys
          coalesce(bactive.all_flags_surveys, 0) as all_flags_surveys, -- all Survey flags
                  coalesce(bactive.sticker_id, 0) as sticker_id,
                  coalesce(bactive.commit2caucus_id, 0) as commit2caucus_id,
                  coalesce(bactive.union_id, 0) as union_id,
                  coalesce(bactive.student_id, 0) as student_id,
                  coalesce(bactive.volunteer_yes_id, 0) as volunteer_yes_id,
          
          -- Bern / Vols
          coalesce(bactive.bernapp, 0) as bernapp, -- Bern users
          coalesce(bactive.slack_vol, 0) as slack_vol, -- Volunteer Slack
          -- ActionKit / Mobilize
             -- RSVPs
          coalesce(bactive.akmob_rsvps, 0) as akmob_rsvps, -- ActionKit Mobilize RSVP count
                  coalesce(bactive.akmob_rsvps_canvass, 0) as akmob_rsvps_canvass,
                  coalesce(bactive.akmob_rsvps_phonebank, 0) as akmob_rsvps_phonebank,
                  coalesce(bactive.akmob_rsvps_small_event, 0) as akmob_rsvps_small_event,
                  coalesce(bactive.akmob_rsvps_friend_to_friend, 0) as akmob_rsvps_friend_to_friend,
                  coalesce(bactive.akmob_rsvps_training, 0) as akmob_rsvps_training,
                  coalesce(bactive.akmob_rsvps_barnstorm, 0) as akmob_rsvps_barnstorm,
                  coalesce(bactive.akmob_rsvps_rally_town_hall, 0) as akmob_rsvps_rally_town_hall,
                  coalesce(bactive.akmob_rsvps_solidarity_action, 0) as akmob_rsvps_solidarity_action,
             -- Attendence
          coalesce(bactive.akmob_attended, 0) as akmob_attended, -- ActionKit Mobilize Attendence count
                  coalesce(bactive.akmob_attended_canvass, 0) as akmob_attended_canvass,
                  coalesce(bactive.akmob_attended_phonebank, 0) as akmob_attended_phonebank,
                  coalesce(bactive.akmob_attended_small_event, 0) as akmob_attended_small_event,
                  coalesce(bactive.akmob_attended_friend_to_friend, 0) as akmob_attended_friend_to_friend,
                  coalesce(bactive.akmob_attended_training, 0) as akmob_attended_training,
                  coalesce(bactive.akmob_attended_barnstorm, 0) as akmob_attended_barnstorm,
                  coalesce(bactive.akmob_attended_rally_town_hall, 0) as akmob_attended_rally_town_hall,
                  coalesce(bactive.akmob_attended_solidarity_action, 0) as akmob_attended_solidarity_action,
          
          -- Donor Basetable
          coalesce(bactive.all_donor_flags, 0) as all_donor_flags, -- Any Donor flag
                  coalesce(bactive.event_signup_0_flag, 0) as event_signup_0_flag,
                  coalesce(bactive.event_signup_1_flag, 0) as event_signup_1_flag,
                  coalesce(bactive.event_signup_1plus_flag, 0) as event_signup_1plus_flag,
                  coalesce(bactive.donor_0_flag, 0) as donor_0_flag,
                  coalesce(bactive.donor_1plus_flag, 0) as donor_1plus_flag,

          -- Activism Flag
          coalesce(bactive.activist_flag, 0) as activist_flag, -- Special activist flag
          coalesce(bhousehold.activist_household_flag,0) as activist_household_flag,
          -- Household Activism / Donor Flags
          coalesce(bactive.any_activist_donor_flag, 0) as any_activist_donor_flag, -- Any of the below flags
          coalesce(bhousehold.any_activist_donor_household_flag,0) as any_activist_donor_household_flag, 
          
          -- Activism in Household
          coalesce(bhousehold.all_flags_myc_household_flag,0) as all_flags_myc_household_flag,
          coalesce(bhousehold.all_flags_surveys_household_flag,0) as all_flags_surveys_household_flag,
          coalesce(bhousehold.akmob_rsvps_household_flag,0) as akmob_rsvps_household_flag,
          coalesce(bhousehold.akmob_attended_household_flag,0) as akmob_attended_household_flag,
          coalesce(bhousehold.bernapp_household_flag,0) as bernapp_household_flag,
          coalesce(bhousehold.slack_vol_household_flag,0) as slack_vol_household_flag,
          -- Donors in Household
          coalesce(bhousehold.donor_1plus_household_flag,0) as donor_1plus_household_flag,

          -- Electorate definition
          CASE WHEN voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                 OR voterinfo.party_8way = '1 - Democrat' 
                 OR voterinfo.civis_2020_partisanship >= .66

                 OR student_flag = 1
                 OR any_activist_donor_flag = 1
                 OR activist_household_flag = 1
                 OR donor_1plus_household_flag = 1

                 OR bvalidate.ccj_id_1 = 1
                 OR bvalidate.thirdp_support_1_id = 1
                 OR bvalidate.thirdp_first_choice_bernie = 1 

                 OR ccj_id_1_hh = 1
                 OR thirdp_support_1_id_hh = 1
                 OR thirdp_first_choice_bernie_hh = 1

                 OR bdcas.donut_segment = '1_core_bernie'
                 OR bdcas.current_support_raw_100 >= 90
                 OR bdcas.field_id_1_score_100 >= 90
                 OR bdcas.field_id_composite_score_100 >= 90
                    THEN '1 - Target universe' 
              ELSE '2 - Non-target' END AS electorate_2way,

          -- Vote readiness bucket
          CASE WHEN electorate_2way = '2 - Non-target' THEN '5 - Non-target'
               WHEN voterinfo.registered_in_state_3way = '1 - Registered in current state' 
                AND voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                AND voterinfo.vote_history_6way IN ('1 - Dem Primary voter (2004-2018)','2 - General voter (2018)','3 - Registered since 2018') 
                    THEN '1 - Vote-ready'
               WHEN voterinfo.registered_in_state_3way = '1 - Registered in current state' 
                AND voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                AND voterinfo.vote_history_6way IN ('4 - Voted in any General (1998-2016)','5 - No vote but eligible (2008-2018)','6 - Other') 
                    THEN '2 - Vote-ready lapsed'
               WHEN (voterinfo.registered_in_state_3way = '1 - Registered in current state' 
                AND voterinfo.dem_primary_eligible_2way = '2 - Must Register as Dem')
                OR  voterinfo.registered_in_state_3way = '2 - Registered in different state'
                    THEN '3 - Register as Dem in current state'
               WHEN voterinfo.registered_in_state_3way = '3 - Absentee voter' 
                AND voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                    THEN '4 - Absentee voter'
               ELSE '5 - Non-target' END AS vote_ready_5way

          -- Turnout hardcode
          ,CASE
               WHEN p.state_code = 'AK' THEN 9670
               WHEN p.state_code = 'AL' THEN 585289
               WHEN p.state_code = 'AR' THEN 342274
               WHEN p.state_code = 'AZ' THEN 571468
               WHEN p.state_code = 'CA' THEN 6018522
               WHEN p.state_code = 'CO' THEN 149356
               WHEN p.state_code = 'CT' THEN 381324
               WHEN p.state_code = 'DC' THEN 153810
               WHEN p.state_code = 'DE' THEN 113819
               WHEN p.state_code = 'FL' THEN 2173879
               WHEN p.state_code = 'GA' THEN 1245209
               WHEN p.state_code = 'HI' THEN 41768
               WHEN p.state_code = 'IA' THEN 250505
               WHEN p.state_code = 'ID' THEN 26167
               WHEN p.state_code = 'IL' THEN 2121029
               WHEN p.state_code = 'IN' THEN 1380352
               WHEN p.state_code = 'KS' THEN 39438
               WHEN p.state_code = 'KY' THEN 751349
               WHEN p.state_code = 'LA' THEN 428631
               WHEN p.state_code = 'MA' THEN 1399670
               WHEN p.state_code = 'MD' THEN 986563
               WHEN p.state_code = 'ME' THEN 47163
               WHEN p.state_code = 'MI' THEN 612426
               WHEN p.state_code = 'MN' THEN 237218
               WHEN p.state_code = 'MO' THEN 886564
               WHEN p.state_code = 'MS' THEN 459992
               WHEN p.state_code = 'MT' THEN 312276
               WHEN p.state_code = 'NC' THEN 1911873
               WHEN p.state_code = 'ND' THEN 22166
               WHEN p.state_code = 'NE' THEN 42171
               WHEN p.state_code = 'NH' THEN 314456
               WHEN p.state_code = 'NJ' THEN 1223933
               WHEN p.state_code = 'NM' THEN 172104
               WHEN p.state_code = 'NV' THEN 152800
               WHEN p.state_code = 'NY' THEN 2022340
               WHEN p.state_code = 'OH' THEN 2460003
               WHEN p.state_code = 'OK' THEN 458344
               WHEN p.state_code = 'OR' THEN 759661
               WHEN p.state_code = 'PA' THEN 2474351
               WHEN p.state_code = 'RI' THEN 196369
               WHEN p.state_code = 'SC' THEN 644796
               WHEN p.state_code = 'SD' THEN 107874
               WHEN p.state_code = 'TN' THEN 708904
               WHEN p.state_code = 'TX' THEN 3644760
               WHEN p.state_code = 'UT' THEN 164423
               WHEN p.state_code = 'VA' THEN 1124566
               WHEN p.state_code = 'VT' THEN 161611
               WHEN p.state_code = 'WA' THEN 296955
               WHEN p.state_code = 'WA' THEN 834590
               WHEN p.state_code = 'WI' THEN 1188457
               WHEN p.state_code = 'WV' THEN 457510
               WHEN p.state_code = 'WY' THEN 9736
               ELSE NULL
           END AS pturnout_2008 
           
           ,CASE
               WHEN p.state_code = 'AK' THEN 10625
               WHEN p.state_code = 'AL' THEN 405741
               WHEN p.state_code = 'AR' THEN 226662
               WHEN p.state_code = 'AZ' THEN 510054
               WHEN p.state_code = 'CA' THEN 5426227
               WHEN p.state_code = 'CO' THEN 133362
               WHEN p.state_code = 'CT' THEN 333325
               WHEN p.state_code = 'DC' THEN 107166
               WHEN p.state_code = 'DE' THEN 97612
               WHEN p.state_code = 'FL' THEN 1859835
               WHEN p.state_code = 'GA' THEN 816925
               WHEN p.state_code = 'HI' THEN 33791
               WHEN p.state_code = 'IA' THEN 174353
               WHEN p.state_code = 'ID' THEN 26110
               WHEN p.state_code = 'IL' THEN 2053564
               WHEN p.state_code = 'IN' THEN 651154
               WHEN p.state_code = 'KS' THEN 40471
               WHEN p.state_code = 'KY' THEN 464737
               WHEN p.state_code = 'LA' THEN 315948
               WHEN p.state_code = 'MA' THEN 1256710
               WHEN p.state_code = 'MD' THEN 950290
               WHEN p.state_code = 'ME' THEN 46938
               WHEN p.state_code = 'MI' THEN 1226753
               WHEN p.state_code = 'MN' THEN 213912
               WHEN p.state_code = 'MO' THEN 640204
               WHEN p.state_code = 'MS' THEN 229262
               WHEN p.state_code = 'MT' THEN 131824
               WHEN p.state_code = 'NC' THEN 1212235
               WHEN p.state_code = 'ND' THEN 3978
               WHEN p.state_code = 'NE' THEN 34208
               WHEN p.state_code = 'NH' THEN 263848
               WHEN p.state_code = 'NJ' THEN 806771
               WHEN p.state_code = 'NM' THEN 221600
               WHEN p.state_code = 'NV' THEN 92320
               WHEN p.state_code = 'NY' THEN 2002696
               WHEN p.state_code = 'OH' THEN 1279774
               WHEN p.state_code = 'OK' THEN 341120
               WHEN p.state_code = 'OR' THEN 709533
               WHEN p.state_code = 'PA' THEN 1693382
               WHEN p.state_code = 'RI' THEN 125167
               WHEN p.state_code = 'SC' THEN 397501
               WHEN p.state_code = 'SD' THEN 53798
               WHEN p.state_code = 'TN' THEN 395424
               WHEN p.state_code = 'TX' THEN 1556166
               WHEN p.state_code = 'UT' THEN 89174
               WHEN p.state_code = 'VA' THEN 808562
               WHEN p.state_code = 'VT' THEN 135765
               WHEN p.state_code = 'WA' THEN 248097
               WHEN p.state_code = 'WA' THEN 865918
               WHEN p.state_code = 'WI' THEN 1025722
               WHEN p.state_code = 'WV' THEN 235437
               WHEN p.state_code = 'WY' THEN 6842
               ELSE NULL
           END AS pturnout_2016

        ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN bvalidate.ccj_id_4 = 1 or bvalidate.ccj_id_5 = 1 then '3 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1
          OR bvalidate.ccj_id_1 = 1
          OR bvalidate.thirdp_support_1_id = 1
          OR bvalidate.thirdp_first_choice_bernie = 1 then '0 - Donors, Activists, Supporters'
        when (bdcas.field_id_1_score_100 >= 70 
          or bdcas.field_id_composite_score_100 >= 70 
          or bdcas.current_support_raw_100 >= 70
          or bdcas.sanders_strong_support_score_100 >= 70 
          or student_flag = 1)
          and bdcas.field_id_5_score_100 < 90 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as support_guardrail
 
        ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN activist_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_flag = 1 
          OR donor_1plus_household_flag = 1 then '0 - Donors and Activists'
        when (bdcas.field_id_1_score_100 >= 70 
          or bdcas.field_id_composite_score_100 >= 70 
          or bdcas.current_support_raw_100 >= 70
          or bdcas.sanders_strong_support_score_100 >= 70 
          or student_flag = 1) 
          and bdcas.field_id_5_score_100 < 90 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as support_guardrail_validation


        /*
        ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN any_activist_donor_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_household_flag = 1 
          OR bvalidate.ccj_id_1 = 1
          OR bvalidate.thirdp_support_1_id = 1
          OR bvalidate.thirdp_first_choice_bernie = 1 then '0 - Donors, Activists, Supporters'
        when bdcas.field_id_1_score_100 >= 80 
          or bdcas.field_id_composite_score_100 >= 80 
          or bdcas.current_support_raw_100 >= 80
          or bdcas.sanders_strong_support_score_100 >= 80
          or spokemodel.spoke_support_1box_100 >= 80 
          or spokemodel.spoke_persuasion_1plus_100 >= 80 
          or volmodel.attendee_100 >= 80 
          or volmodel.kickoff_party_rally_barnstorm_attendee_100 >= 80 
          or volmodel.canvasser_phonebank_attendee_100 >= 80 
          or volmodel.bernie_action_100  >= 80
          or student_flag = 1
         and bdcas.field_id_5_score_100 <= 80 
         and bdcas.biden_support_100 <= 90 
         and spokemodel.spoke_persuasion_1minus_100 <= 80 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as support_guardrail_extra

     
        ,case 
        WHEN electorate_2way = '2 - Non-target' then '3 - Non-target' 
        WHEN any_activist_donor_flag = 1
          OR activist_household_flag = 1
          OR donor_1plus_household_flag = 1 then '0 - Donors and Activists'
        when bdcas.field_id_1_score_100 >= 80 
          or bdcas.field_id_composite_score_100 >= 80 
          or bdcas.current_support_raw_100 >= 80
          or bdcas.sanders_strong_support_score_100 >= 80
          or spokemodel.spoke_support_1box_100 >= 80 
          or spokemodel.spoke_persuasion_1plus_100 >= 80 
          or volmodel.attendee_100 >= 80 
          or volmodel.kickoff_party_rally_barnstorm_attendee_100 >= 80 
          or volmodel.canvasser_phonebank_attendee_100 >= 80 
          or volmodel.bernie_action_100  >= 80
          or student_flag = 1
         and bdcas.field_id_5_score_100 <= 80 
         and bdcas.biden_support_100 <= 90 
         and spokemodel.spoke_persuasion_1minus_100 <= 80 then '1 - Inside Support Guardrail'
        else '2 - Outside Support Guardrail' end as support_guardrail_extra_validation
        */

           /*
           -- Support thresholds   
           case 
              when electorate_2way = '2 - Non-target' THEN '5 - Non-target' 
              when (bdcas.biden_support_100 <= 80 or spokemodel.spoke_persuasion_1minus_100 <= 50) and (bdcas.current_support_raw_100 >= 90 or bdcas.sanders_strong_support_score_100 >= 80) and (bdcas.field_id_1_score_100 >= 80 or bdcas.field_id_composite_score_100 >= 80) and  (spokemodel.spoke_support_1box_100 >= 75 and spokemodel.spoke_persuasion_1plus_100 >= 75) and (volmodel.attendee_100 >= 80 or volmodel.kickoff_party_rally_barnstorm_attendee_100 >= 80 or volmodel.canvasser_phonebank_attendee_100 >= 80 or volmodel.bernie_action_100  >= 80) then '1 - Support Tier 1'
              when (bdcas.biden_support_100 <= 80 or spokemodel.spoke_persuasion_1minus_100 <= 50) and (bdcas.current_support_raw_100 >= 80 or bdcas.sanders_strong_support_score_100 >= 80 or bdcas.field_id_1_score_100 >= 80 or bdcas.field_id_composite_score_100 >= 80) and  (spokemodel.spoke_support_1box_100 >= 75 or spokemodel.spoke_persuasion_1plus_100 >= 75) and (volmodel.attendee_100 >= 80 or volmodel.kickoff_party_rally_barnstorm_attendee_100 >= 80 or volmodel.canvasser_phonebank_attendee_100 >= 80 or volmodel.bernie_action_100  >= 80) then '2 - Support Tier 2'
              when (bdcas.biden_support_100 <= 90 or spokemodel.spoke_persuasion_1minus_100 <= 60) and (bdcas.current_support_raw_100 >= 80 or bdcas.sanders_strong_support_score_100 >= 80 or bdcas.field_id_1_score_100 >= 80 or bdcas.field_id_composite_score_100 >= 80 or spokemodel.spoke_support_1box_100 >= 80 or spokemodel.spoke_persuasion_1plus_100 >= 80 or volmodel.attendee_100 >= 80 or volmodel.kickoff_party_rally_barnstorm_attendee_100 >= 80 or volmodel.canvasser_phonebank_attendee_100 >= 80 or volmodel.bernie_action_100  >= 80) then '3 - Support Tier 3'
           else '4 - Support Tier 4' end as score_thresholds

           ,CASE 
           WHEN score_thresholds = '5 - Non-target' then '4 - Non-target' 
           WHEN activist_flag = 1
             OR activist_household_flag = 1
             OR donor_1plus_flag = 1 
             OR donor_1plus_household_flag = 1 then '0 - Donors and Activists'
           WHEN score_thresholds = '1 - Support Tier 1' then '1 - Support Tier 1'
           WHEN score_thresholds = '2 - Support Tier 2' then '2 - Support Tier 2'
           WHEN score_thresholds = '3 - Support Tier 3' then '3 - Support Tier 3'
           ELSE '4 - Non-target' END AS support_tier_validation

           ,CASE 
           WHEN score_thresholds = '5 - Non-target' 
             or bvalidate.ccj_id_5 = 1 
             or bvalidate.thirdp_support_5_id = 1 
             or bvalidate.thirdp_first_choice_biden_warren_buttigieg = 1 then '4 - Non-target' 
           WHEN bvalidate.ccj_id_1 = 1 
             or bvalidate.thirdp_first_choice_bernie = 1 
             or bvalidate.thirdp_support_1_id = 1 
             or support_tier_validation = '0 - Donors and Activists' then '0 - Donors, Activists, Support 1 IDs'
           WHEN score_thresholds = '1 - Support Tier 1' then '1 - Support Tier 1'
           WHEN score_thresholds = '2 - Support Tier 2' then '2 - Support Tier 2'
           WHEN score_thresholds = '3 - Support Tier 3' or bvalidate.ccj_id_2 = 1 or bvalidate.thirdp_support_2_id = 1 then '3 - Support Tier 3'
           ELSE '4 - Non-target' END AS support_tier_juiced
           */

           --,round(1.0*sum((case when electorate_2way  = '1 - Target universe' then p.person_id end)) OVER (partition BY p.state_code ORDER BY support_tier_juiced ASC, bdcas.field_id_1_score DESC ROWS UNBOUNDED PRECEDING)/pturnout_2016,4) AS rolling_electorate_share
           --,row_number() OVER (PARTITION BY p.state_code ORDER BY support_tier_juiced ASC, bdcas.field_id_1_score DESC) as support_tier_juiced_rank
        
     FROM 

 -- PERSON
      (SELECT person_id::varchar,
              state_code,
              voting_address_id::varchar,
              county_fips,
              county_name,
              dnc_precinct_id,
              van_precinct_id,
              voting_address_latitude,
              voting_address_longitude
      FROM phoenix_analytics.person
      WHERE is_deceased = 'f'
        AND reg_record_merged = 'f'
        AND reg_on_current_file = 't'
        AND reg_voter_flag = 't'
        --AND state_code in ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA')
        ) p

-- CROSSWALK
    LEFT JOIN
    (SELECT * FROM 
    	(SELECT person_id, 
                myc_van_id,
                st_myc_van_id,
                myv_van_id,
                st_myv_van_id,
                bern_canvasser_id,
                actionkit_id, 
                jsonid_encoded, 
                student_hash,
                row_number() OVER (PARTITION BY person_id ORDER BY actionkit_id NULLS LAST) AS rn 
    	FROM bernie_data_commons.master_xwalk_dnc) 
    WHERE rn = 1
    ) xwalk
    using(person_id)

-- SUPPORT SCORES
      LEFT JOIN
     (SELECT person_id::varchar,
             donut_segment,
             coalesce(current_support_raw/100,0) AS current_support_raw,
             coalesce(current_support_rank_order,1) as current_support_raw_100,
             coalesce(field_id_1_score/100,0) AS field_id_1_score,
             coalesce(field_id_1_score_ntile,1) as field_id_1_score_100,
             coalesce(field_id_composite_score/100,0)  AS field_id_composite_score,
             coalesce(field_id_composite_score_ntile,1) as field_id_composite_score_100,
             coalesce(turnout_current/100,0) AS turnout_current,
             coalesce(turnout_current_ntile,1) as turnout_current_100,
             coalesce(sanders_strong_support_score/100,0) AS sanders_strong_support_score,
             coalesce(sanders_strong_support_score_ntile,1) as sanders_strong_support_score_100,
             coalesce(sanders_very_excited_score/100,0) AS sanders_very_excited_score,
             coalesce(sanders_very_excited_score_ntile,1) as sanders_very_excited_score_100,
             coalesce(biden_support/100,0) AS biden_support,
             coalesce(biden_support_ntile,1) as biden_support_100,
             coalesce(warren_support/100,0) AS warren_support,
             coalesce(warren_support_ntile,1) as warren_support_100,
             coalesce(buttigieg_support/100,0) AS buttigieg_support,
             coalesce(buttigieg_support_ntile,1) as buttigieg_support_100,
             coalesce(trump_support/100,0) as trump_support,
             coalesce(trump_support_ntile,1) as trump_support_100,
             coalesce(bloomberg_support/100,0) as bloomberg_support,
             coalesce(bloomberg_support_ntile,1) as bloomberg_support_100,
             coalesce(klobuchar_support/100,0) as klobuchar_support,
             coalesce(klobuchar_support_ntile,1) as klobuchar_support_100,
             coalesce(steyer_support/100,0) as steyer_support,
             coalesce(steyer_support_ntile,1) as steyer_support_100,
             coalesce(field_id_5_score/100,0) as field_id_5_score,
             coalesce(field_id_5_score_ntile,1) as field_id_5_score_100
      FROM bernie_data_commons.all_scores_ntile) bdcas
     using(person_id)

-- SPOKE MODELS
   LEFT JOIN
    (SELECT person_id::varchar,
            spoke_support_1box,
            spoke_persuasion_1plus,
            spoke_persuasion_1minus,
            spoke_support_1box_100,
            spoke_persuasion_1plus_100,
            spoke_persuasion_1minus_100 
    FROM scores.spoke_output_20191221) spokemodel
    using(person_id)

-- VOLUNTEER MODELS
   LEFT JOIN
    (SELECT person_id,
            attendee,
            kickoff_party_rally_barnstorm_attendee,
            canvasser_phonebank_attendee,
            bernie_action,
            attendee_100,
            kickoff_party_rally_barnstorm_attendee_100,
            canvasser_phonebank_attendee_100,
            bernie_action_100
    FROM scores.actionpop_output_20191220) volmodel
    using(person_id)

-- DONOR/VOLUNTEER HOUSEHOLD LEVEL INFO
	left join
   (select voting_address_id,
           any_activist_donor_household_flag, 
           activist_household_flag,
           all_flags_myc_household_flag,
           all_flags_surveys_household_flag,
           akmob_rsvps_household_flag,
           akmob_attended_household_flag,
           bernapp_household_flag,
           slack_vol_household_flag,
           donor_1plus_household_flag,
           ccj_id_1_hh,
           ccj_id_2_hh,
           ccj_id_3_hh,
           ccj_id_4_hh,
           ccj_id_5_hh,
           ccj_id_1_2_3_4_5_hh,
           thirdp_first_choice_bernie_hh,
           thirdp_first_choice_trump_hh,
           thirdp_first_choice_biden_warren_buttigieg_hh,
           thirdp_first_choice_any_hh,
           thirdp_support_1_id_hh,
           thirdp_support_2_id_hh,
           thirdp_support_3_id_hh,
           thirdp_support_4_id_hh,
           thirdp_support_5_id_hh,
           thirdp_support_1_2_3_4_5_id_hh
   from bernie_nmarchio2.base_household) bhousehold
   on p.voting_address_id = bhousehold.voting_address_id     

-- DONOR/VOLUNTEER INFO
    left join
   (SELECT person_id,
           activist_flag,
           any_activist_donor_flag,

           all_flags_myc,
           mvp_myc,
           activist_myc,
           canvass_myc,
           volunteer_myc,
           constituencies_myc,
           volunteer_shifts_myc,
           student_myc,
           teacher_myc,
           labor_myc,

           all_flags_surveys,
           sticker_id,
           commit2caucus_id,
           union_id,
           student_id,
           volunteer_yes_id,

           bernapp,
           bern_student,
           bern_union,

           slack_vol,

           akmob_rsvps,
           akmob_rsvps_canvass,
           akmob_rsvps_phonebank,
           akmob_rsvps_small_event,
           akmob_rsvps_friend_to_friend,
           akmob_rsvps_training,
           akmob_rsvps_barnstorm,
           akmob_rsvps_rally_town_hall,
           akmob_rsvps_solidarity_action,

           akmob_attended,
           akmob_attended_canvass,
           akmob_attended_phonebank,
           akmob_attended_small_event,
           akmob_attended_friend_to_friend,
           akmob_attended_training,
           akmob_attended_barnstorm,
           akmob_attended_rally_town_hall,
           akmob_attended_solidarity_action,

           all_donor_flags,
           event_signup_0_flag,
           event_signup_1_flag,
           event_signup_1plus_flag,
           donor_0_flag,
           donor_1plus_flag

   FROM bernie_nmarchio2.base_activists) bactive
   using(person_id)

-- FIELD AND THIRD PARTY IDS
   LEFT JOIN 
   (SELECT * FROM bernie_nmarchio2.base_validation) bvalidate
    using(person_id)


-- NATIONAL CONSTITUENCY TABLE
LEFT JOIN 
  (SELECT person_id::varchar,
          flag_muslim,
          flag_student_age,
          flag_union,
          flag_veteran
   FROM bernie_data_commons.national_constituency_table) nct
  using(person_id)

-- VOTER INFO
   LEFT JOIN
     (SELECT rainbo.person_id::varchar,
             rainbo.vote_history_6way,
             rainbo.early_vote_history_3way,
             rainbo.registered_in_state_3way,
             rainbo.dem_primary_eligible_2way,
             rainbo.party_8way,
             rainbo.party_3way,
             rainbo.race_5way,
             rainbo.spanish_language_2way,
             rainbo.age_5way,
             rainbo.ideology_5way,
             rainbo.education_2way,
             rainbo.income_5way,
             rainbo.gender_2way,
             rainbo.urban_3way,
             rainbo.child_in_hh_2way,
             rainbo.marital_2way,
             rainbo.religion_9way,
             rainbo.early_voting_3way,
             rainbo.absentee_voting_4way,
             as20.civis_2020_partisanship::FLOAT8,
             as18.civis_2018_economic_persuasion::FLOAT8,
             as18.civis_2018_one_pct_persuasion::FLOAT8,
             as18.civis_2018_marijuana_persuasion::FLOAT8,
             as18.civis_2018_college_persuasion::FLOAT8,
             as18.civis_2018_welcome_persuasion::FLOAT8,
             as18.civis_2018_sexual_assault_persuasion::FLOAT8         

      --FROM phoenix_analytics.person p
      from
      (select person_id::varchar,
              vote_history_6way,
              early_vote_history_3way,
              registered_in_state_3way,
              dem_primary_eligible_2way,
              party_8way,
              party_3way,
              race_5way,
              spanish_language_2way,
              age_5way,
              ideology_5way,
              education_2way,
              income_5way,
              gender_2way,
              urban_3way,
              child_in_hh_2way,
              marital_2way,
              religion_9way,
              early_voting_3way,
              absentee_voting_4way FROM bernie_data_commons.rainbow_analytics_frame) rainbo
      LEFT JOIN 
      (select person_id, 
      	      civis_2020_partisanship from phoenix_scores.all_scores_2020) as20 
      using(person_id)
      LEFT JOIN 
      (select person_id, 
              civis_2018_economic_persuasion::FLOAT8,
              civis_2018_one_pct_persuasion::FLOAT8,
              civis_2018_marijuana_persuasion::FLOAT8,
              civis_2018_college_persuasion::FLOAT8,
              civis_2018_welcome_persuasion::FLOAT8,
              civis_2018_sexual_assault_persuasion::FLOAT8 from phoenix_scores.all_scores_2018) as18 
      using(person_id)
      ) voterinfo 
     using(person_id)
);
commit;

-- Drop staging tables and run grants
drop table if exists bernie_nmarchio2.base_akmobevents;
drop table if exists bernie_nmarchio2.base_activists;
drop table if exists bernie_nmarchio2.base_household;
drop table if exists bernie_nmarchio2.base_validation;
drop table if exists bernie_nmarchio2.base_universe;

grant select on bernie_data_commons.base_universe to group bernie_data;
