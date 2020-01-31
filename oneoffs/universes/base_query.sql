begin;

set wlm_query_slot_count to 5;

-- ActionKit / Mobilize
CREATE temp table akmobevents as
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
    Left JOIN
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
          ON mob.mobilize_id = xwalk.mobilize_id AND xwalk.rownum = 1) WHERE rownum = 1 and (ak_event_id is not null and mobilize_id is not null) and event_recode is not null);

DROP TABLE IF EXISTS bernie_nmarchio2.base_universe;
CREATE TABLE bernie_nmarchio2.base_universe
distkey(person_id) 
sortkey(person_id) AS
  (SELECT p.person_id::varchar,
  	      xwalk.actionkit_id, 
  	      xwalk.jsonid,
  	      p.voting_address_id,

  	      -- Geographic codes
          p.state_code,
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
          voterinfo.registered_in_state_3way,
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

          -- Support scores
          bdcas.donut_segment,
          bdcas.current_support_raw,
          bdcas.current_support_raw_100,
          bdcas.field_id_1_score,
          bdcas.field_id_1_score_100,
          bdcas.field_id_composite_score,
          bdcas.field_id_composite_score_100,
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
          round((bdcas.current_support_raw * bdcas.turnout_current),4) AS bernie_net_votes_current,
 
          -- Message targeting
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
          thirdp.thirdp_survey_date,
          thirdp.thirdp_first_choice_bernie,
          thirdp.thirdp_first_choice_trump,
          thirdp.thirdp_first_choice_biden,
          thirdp.thirdp_first_choice_warren,
          thirdp.thirdp_first_choice_buttigieg,
          thirdp.thirdp_first_choice_biden_warren_buttigieg,
          thirdp.thirdp_first_choice_any,
          thirdp.thirdp_support_1_id,
          thirdp.thirdp_support_2_id,
          thirdp.thirdp_support_1_2_id,
          thirdp.thirdp_support_3_id,
          thirdp.thirdp_support_4_id,
          thirdp.thirdp_support_5_id,
          thirdp.thirdp_support_1_2_3_4_5_id,
          thirdp.thirdp_holdout_id,

          -- Field IDs
          ccj.ccj_contactdate,
          ccj.ccj_contact_made,
          ccj.ccj_negative_result,
          ccj.ccj_id_1,
          ccj.ccj_id_1_2,
          ccj.ccj_id_2,
          ccj.ccj_id_3,
          ccj.ccj_id_4,
          ccj.ccj_id_5,
          ccj.ccj_id_1_2_3_4_5,
          ccj.ccj_holdout_id,
          CASE WHEN id1_household = 1 AND ccj.ccj_id_1 <> 1 THEN 1 ELSE 0 END AS id1_household ,

          -- Donor / Activism Flags
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
          coalesce(akmob.akmob_attended_canvass ,0) as akmob_attended_canvass ,
          coalesce(akmob.akmob_attended_phonebank ,0) as akmob_attended_phonebank ,
          coalesce(akmob.akmob_attended_small_event ,0) as akmob_attended_small_event ,
          coalesce(akmob.akmob_attended_friend_to_friend ,0) as akmob_attended_friend_to_friend ,
          coalesce(akmob.akmob_attended_training ,0) as akmob_attended_training ,
          coalesce(akmob.akmob_attended_barnstorm ,0) as akmob_attended_barnstorm ,
          coalesce(akmob.akmob_attended_rally_town_hall ,0) as akmob_attended_rally_town_hall ,
          coalesce(akmob.akmob_attended_solidarity_action,0) as akmob_attended_solidarity_action,
          coalesce(p_household.donor_household,0) as donor_household,
          coalesce(p_household.event_household,0) as event_household,
          coalesce(p_household.volunteer_household,0) as volunteer_household,

          case 
          when mvp_myc = 1
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
               or slack_vol = 1
               or akmob_attended_canvass = 1
               or akmob_attended_phonebank = 1
               or akmob_attended_small_event = 1
               or akmob_attended_friend_to_friend = 1
               or akmob_attended_training = 1
               or akmob_attended_barnstorm = 1
               or akmob_attended_rally_town_hall = 1
               or akmob_attended_solidarity_action = 1
               or donor_household = 1
               or event_household = 1
               or volunteer_household = 1 then 1
          else 0 end as activist_donor_flag,

          -- Electorate definition
          CASE WHEN voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                    OR voterinfo.party_8way = '1 - Democratic' 
                    OR voterinfo.civis_2020_partisanship >= .66
                    OR activist_donor_flag = 1
                    OR id1_household = 1
                    OR ccj.ccj_id_1_2 = 1
                    OR thirdp.thirdp_support_1_2_id = 1
                    OR thirdp.thirdp_first_choice_bernie = 1 
                    OR bdcas.donut_segment = '1_core_bernie'
                    OR bdcas.current_support_raw_100 >= 90
                    OR bdcas.field_id_1_score_100 >= 90
                    OR bdcas.field_id_composite_score_100 >= 90
                    THEN '1 - Target universe' 
              ELSE '2 - Non-target' END AS electorate_2way,

          -- Vote readiness bucket
          CASE
              WHEN     electorate_2way = '2 - Non-target' 
                       THEN '6 - Non-target'
              WHEN     voterinfo.registered_in_state_3way = '1 - Registered in current state' 
                   AND voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                   AND voterinfo.vote_history_6way IN ('1 - Dem Primary voter (2004-2018)','2 - General voter (2018)','3 - Registered since 2018')
                       THEN '1 - Vote-ready'
              WHEN     voterinfo.registered_in_state_3way = '1 - Registered in current state' 
                   AND voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                   AND voterinfo.vote_history_6way IN ('4 - Voted in any General (1998-2016)','5 - No vote but eligible (2008-2018)','6 - Other')
                       THEN '2 - Vote-ready lapsed'
              WHEN     voterinfo.registered_in_state_3way = '1 - Registered in current state' 
                   AND voterinfo.dem_primary_eligible_2way = '2 - Must Register as Dem'
                       THEN '3 - Register as Dem'
              WHEN     voterinfo.registered_in_state_3way = '2 - Registered in different state'
                       THEN '4 - Register in current state'
              WHEN     voterinfo.registered_in_state_3way = '3 - Absentee voter' 
                   AND voterinfo.dem_primary_eligible_2way = '1 - Dem Primary Eligible' 
                       THEN '5 - Absentee voter'
              ELSE '6 - Non-target' END AS vote_ready_6way,

           -- Support thresholds   
           case
              when electorate_2way = '2 - Non-target' THEN '5 - Non-target' 
              when (field_id_composite_score_100 <= 90 or field_id_1_score_100 <= 90) and (biden_support_100 >= 70 AND warren_support_100 >= 70 AND buttigieg_support_100 >= 70 AND spoke_persuasion_1minus_100 >= 70) THEN '4 - Avoid'
              when (current_support_raw_100 >= 90 or field_id_composite_score_100 >= 90 or field_id_1_score_100 >= 90) then '1 - Support Tier 1'
              when (current_support_raw_100 >= 80 or field_id_composite_score_100 >= 80 or field_id_1_score_100 >= 80) then '2 - Support Tier 2' 
           else '3 - Support Tier 3' end as score_thresholds

          -- Turnout hardcode
          ,CASE
               WHEN state_code = 'AK' THEN 9670
               WHEN state_code = 'AL' THEN 585289
               WHEN state_code = 'AR' THEN 342274
               WHEN state_code = 'AZ' THEN 571468
               WHEN state_code = 'CA' THEN 6018522
               WHEN state_code = 'CO' THEN 149356
               WHEN state_code = 'CT' THEN 381324
               WHEN state_code = 'DC' THEN 153810
               WHEN state_code = 'DE' THEN 113819
               WHEN state_code = 'FL' THEN 2173879
               WHEN state_code = 'GA' THEN 1245209
               WHEN state_code = 'HI' THEN 41768
               WHEN state_code = 'IA' THEN 250505
               WHEN state_code = 'ID' THEN 26167
               WHEN state_code = 'IL' THEN 2121029
               WHEN state_code = 'IN' THEN 1380352
               WHEN state_code = 'KS' THEN 39438
               WHEN state_code = 'KY' THEN 751349
               WHEN state_code = 'LA' THEN 428631
               WHEN state_code = 'MA' THEN 1399670
               WHEN state_code = 'MD' THEN 986563
               WHEN state_code = 'ME' THEN 47163
               WHEN state_code = 'MI' THEN 612426
               WHEN state_code = 'MN' THEN 237218
               WHEN state_code = 'MO' THEN 886564
               WHEN state_code = 'MS' THEN 459992
               WHEN state_code = 'MT' THEN 312276
               WHEN state_code = 'NC' THEN 1911873
               WHEN state_code = 'ND' THEN 22166
               WHEN state_code = 'NE' THEN 42171
               WHEN state_code = 'NH' THEN 314456
               WHEN state_code = 'NJ' THEN 1223933
               WHEN state_code = 'NM' THEN 172104
               WHEN state_code = 'NV' THEN 152800
               WHEN state_code = 'NY' THEN 2022340
               WHEN state_code = 'OH' THEN 2460003
               WHEN state_code = 'OK' THEN 458344
               WHEN state_code = 'OR' THEN 759661
               WHEN state_code = 'PA' THEN 2474351
               WHEN state_code = 'RI' THEN 196369
               WHEN state_code = 'SC' THEN 644796
               WHEN state_code = 'SD' THEN 107874
               WHEN state_code = 'TN' THEN 708904
               WHEN state_code = 'TX' THEN 3644760
               WHEN state_code = 'UT' THEN 164423
               WHEN state_code = 'VA' THEN 1124566
               WHEN state_code = 'VT' THEN 161611
               WHEN state_code = 'WA' THEN 296955
               WHEN state_code = 'WA' THEN 834590
               WHEN state_code = 'WI' THEN 1188457
               WHEN state_code = 'WV' THEN 457510
               WHEN state_code = 'WY' THEN 9736
               ELSE NULL
           END AS pturnout_2008 

           ,CASE
               WHEN state_code = 'AK' THEN 10625
               WHEN state_code = 'AL' THEN 405741
               WHEN state_code = 'AR' THEN 226662
               WHEN state_code = 'AZ' THEN 510054
               WHEN state_code = 'CA' THEN 5426227
               WHEN state_code = 'CO' THEN 133362
               WHEN state_code = 'CT' THEN 333325
               WHEN state_code = 'DC' THEN 107166
               WHEN state_code = 'DE' THEN 97612
               WHEN state_code = 'FL' THEN 1859835
               WHEN state_code = 'GA' THEN 816925
               WHEN state_code = 'HI' THEN 33791
               WHEN state_code = 'IA' THEN 174353
               WHEN state_code = 'ID' THEN 26110
               WHEN state_code = 'IL' THEN 2053564
               WHEN state_code = 'IN' THEN 651154
               WHEN state_code = 'KS' THEN 40471
               WHEN state_code = 'KY' THEN 464737
               WHEN state_code = 'LA' THEN 315948
               WHEN state_code = 'MA' THEN 1256710
               WHEN state_code = 'MD' THEN 950290
               WHEN state_code = 'ME' THEN 46938
               WHEN state_code = 'MI' THEN 1226753
               WHEN state_code = 'MN' THEN 213912
               WHEN state_code = 'MO' THEN 640204
               WHEN state_code = 'MS' THEN 229262
               WHEN state_code = 'MT' THEN 131824
               WHEN state_code = 'NC' THEN 1212235
               WHEN state_code = 'ND' THEN 3978
               WHEN state_code = 'NE' THEN 34208
               WHEN state_code = 'NH' THEN 263848
               WHEN state_code = 'NJ' THEN 806771
               WHEN state_code = 'NM' THEN 221600
               WHEN state_code = 'NV' THEN 92320
               WHEN state_code = 'NY' THEN 2002696
               WHEN state_code = 'OH' THEN 1279774
               WHEN state_code = 'OK' THEN 341120
               WHEN state_code = 'OR' THEN 709533
               WHEN state_code = 'PA' THEN 1693382
               WHEN state_code = 'RI' THEN 125167
               WHEN state_code = 'SC' THEN 397501
               WHEN state_code = 'SD' THEN 53798
               WHEN state_code = 'TN' THEN 395424
               WHEN state_code = 'TX' THEN 1556166
               WHEN state_code = 'UT' THEN 89174
               WHEN state_code = 'VA' THEN 808562
               WHEN state_code = 'VT' THEN 135765
               WHEN state_code = 'WA' THEN 248097
               WHEN state_code = 'WA' THEN 865918
               WHEN state_code = 'WI' THEN 1025722
               WHEN state_code = 'WV' THEN 235437
               WHEN state_code = 'WY' THEN 6842
               ELSE NULL
           END AS pturnout_2016

     FROM 

 -- PERSON
      (SELECT person_id::varchar,
              state_code,
              voting_address_id,
              county_fips,
              county_name,
              dnc_precinct_id,
              van_precinct_id,
              voting_address_latitude,
              voting_address_longitude
      FROM phoenix_analytics.person
      WHERE is_deceased = FALSE
        AND reg_record_merged = FALSE
        AND reg_on_current_file = TRUE
        AND reg_voter_flag = TRUE 
        AND state_code IN ('AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA')) p

-- CROSSWALK
    LEFT JOIN
    (SELECT * FROM (SELECT person_id, actionkit_id, jsonid, row_number() OVER (PARTITION BY person_id ORDER BY actionkit_id NULLS LAST) AS rn FROM bernie_data_commons.master_xwalk_dnc) WHERE rn = 1) xwalk
    using(person_id)

-- SUPPORT SCORES
      LEFT JOIN
     (SELECT person_id::varchar,
             donut_segment,
             current_support_raw/100 AS current_support_raw,
             current_support_rank_order as current_support_raw_100,
             field_id_1_score/100 AS field_id_1_score,
             field_id_1_score_ntile as field_id_1_score_100,
             field_id_composite_score/100  AS field_id_composite_score,
             field_id_composite_score_ntile as field_id_composite_score_100,
             turnout_current/100 AS turnout_current,
             turnout_current_ntile as turnout_current_100,
             sanders_strong_support_score/100 AS sanders_strong_support_score,
             sanders_strong_support_score_ntile as sanders_strong_support_score_100,
             sanders_very_excited_score/100 AS sanders_very_excited_score,
             sanders_very_excited_score_ntile as sanders_very_excited_score_100,
             biden_support/100 AS biden_support,
             biden_support_ntile as biden_support_100,
             warren_support/100 AS warren_support,
             warren_support_ntile as warren_support_100,
             buttigieg_support/100 AS buttigieg_support,
             buttigieg_support_ntile as buttigieg_support_100
      FROM bernie_data_commons.all_scores_ntile) bdcas
     using(person_id)

-- FIELD IDS
   LEFT JOIN 
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
    
            ,COUNT(distinct case when
                   datediff(d, '2020-01-17', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state = 'MN' or
                   datediff(d, '2020-01-22', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state IN ('TN','AR','MO','AL','KY','GA','MS') or
                   datediff(d, '2019-12-01', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state NOT IN ('MN','TN','AR','MO','AL','KY','GA','MS','NH','NV','IA','SC') THEN person_id END) as validtime
            ,max(TO_DATE(contactdate, 'YYYY-MM-DD')) AS ccj_contactdate
            ,COUNT(distinct CASE WHEN resultcode IN ('Canvassed', 'Do Not Contact', 'Refused', 'Call Back', 'Language Barrier', 'Hostile', 'Come Back', 'Cultivation', 'Refused Contact', 'Spanish', 'Other', 'Not Interested') THEN person_id END) AS ccj_contact_made 
            ,COUNT(distinct CASE WHEN resultcode IN ('Do Not Contact','Hostile','Refused','Refused Contact') OR (support_int = 4 OR support_int = 5) THEN person_id END) AS ccj_negative_result 
            ,COUNT(DISTINCT CASE WHEN support_int = 1 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_1 
            ,COUNT(DISTINCT CASE WHEN support_int IN (1,2) AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_1_2 
            ,COUNT(DISTINCT CASE WHEN support_int = 2 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_2 
            ,COUNT(DISTINCT CASE WHEN support_int = 3 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_3 
            ,COUNT(DISTINCT CASE WHEN support_int = 4 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_4 
            ,COUNT(DISTINCT CASE WHEN support_int = 5 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_5 
            ,COUNT(DISTINCT CASE WHEN support_int IN (1,2,3,4,5) AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_1_2_3_4_5
            FROM bernie_data_commons.ccj_dnc 
            WHERE unique_id_flag = 1 AND person_id IS NOT NULL GROUP BY 1) ccj
            LEFT JOIN 
            (select person_id::varchar, 1 as holdout from haystaq.set_no_current where set_no=3) ho
            using(person_id) where person_id is not null) ccj
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
    (select 
     person_id::varchar(10)
          ,COUNT(distinct case when
               datediff(d, '2020-01-17', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state = 'MN' or
               datediff(d, '2020-01-22', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state IN ('TN','AR','MO','AL','KY','GA','MS') or
               datediff(d, '2019-12-01', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state NOT IN ('MN','TN','AR','MO','AL','KY','GA','MS','NH','NV','IA','SC') then person_id END) as validtime
          ,max(TO_DATE(survey_date, 'YYYY-MM-DD')) AS thirdp_survey_date
          ,COUNT(distinct case when first_choice = 'Bernie Sanders' then person_id end) as thirdp_first_choice_bernie
          ,COUNT(distinct case when first_choice = 'Donald Trump' then person_id end) as thirdp_first_choice_trump
          ,COUNT(distinct case when first_choice = 'Joe Biden' then person_id end) as thirdp_first_choice_biden
          ,COUNT(distinct case when first_choice = 'Elizabeth Warren' then person_id end) as thirdp_first_choice_warren
          ,COUNT(distinct case when first_choice = 'Pete Buttigieg' then person_id end) as thirdp_first_choice_buttigieg
          ,COUNT(distinct case when first_choice IN ('Pete Buttigieg','Elizabeth Warren','Joe Biden') then person_id end) as thirdp_first_choice_biden_warren_buttigieg
          ,COUNT(distinct case when first_choice IS NOT NULL then person_id end) as thirdp_first_choice_any
          ,COUNT(distinct case when support_int = 1 then person_id end) as thirdp_support_1_id
          ,COUNT(distinct case when support_int = 2 then person_id end) as thirdp_support_2_id
          ,COUNT(distinct case when support_int IN (1,2) then person_id end) as thirdp_support_1_2_id
          ,COUNT(distinct case when support_int = 3 then person_id end) as thirdp_support_3_id
          ,COUNT(distinct case when support_int = 4 then person_id end) as thirdp_support_4_id
          ,COUNT(distinct case when support_int = 5 then person_id end) as thirdp_support_5_id
          ,COUNT(distinct case when support_int IN (1,2,3,4,5) then person_id end) as thirdp_support_1_2_3_4_5_id
    FROM bernie_data_commons.third_party_ids group by 1) third
   LEFT JOIN
    (select person_id::varchar, 1 as holdout from haystaq.set_no_current where set_no=3) ho 
    using(person_id) where person_id is not null) thirdp
    using(person_id)

-- SPOKE AND VOL MODELS
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

-- VOLUNTEER INFO
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
           (SELECT person_id::varchar(10), bern_id, bern_canvasser_id FROM bernie_data_commons.master_xwalk) xwalk ON canv.id = xwalk.bern_id where person_id is not null
           GROUP BY 1) bern 
     using(person_id)
    
    left join
    -- Survey Responses
    (select * from 
    (SELECT person_id::varchar(10) ,
            count(distinct CASE WHEN surveyresponseid IN (40) THEN 1 WHEN surveyresponseid IN (106,107) THEN person_id END) AS sticker_id ,
            count(distinct CASE WHEN surveyresponseid IN (99) THEN 1 WHEN surveyresponseid IN (100,101) THEN person_id END) AS commit2caucus_id ,
            count(distinct CASE WHEN surveyresponseid IN (10) THEN person_id END) AS union_id ,
            count(distinct CASE WHEN surveyresponseid IN (9,103,104) THEN person_id END) AS student_id ,
            count(distinct CASE WHEN surveyresponseid IN (32) THEN 1 WHEN surveyresponseid IN (33) THEN person_id END) AS volunteer_yes_id 
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
           count(distinct CASE WHEN event_recode = 'canvass' THEN person_id END) AS akmob_attended_canvass ,
           count(distinct CASE WHEN event_recode = 'phonebank' THEN person_id END) AS akmob_attended_phonebank ,
           count(distinct CASE WHEN event_recode = 'small-event' THEN person_id END) AS akmob_attended_small_event ,
           count(distinct CASE WHEN event_recode = 'friend-to-friend' THEN person_id END) AS akmob_attended_friend_to_friend ,
           count(distinct CASE WHEN event_recode = 'training' THEN person_id END) AS akmob_attended_training ,
           count(distinct CASE WHEN event_recode = 'barnstorm' THEN person_id END) AS akmob_attended_barnstorm ,
           count(distinct CASE WHEN event_recode = 'rally-town-hall' THEN person_id END) AS akmob_attended_rally_town_hall ,
           count(distinct CASE WHEN event_recode = 'solidarity-action' THEN person_id END) AS akmob_attended_solidarity_action 
           from (
    (select distinct person_id, event_recode from
      (SELECT DISTINCT user_id::varchar(256) AS mobilize_id ,
                       event_id::varchar(256) AS mobilize_event_id 
       FROM mobilize.participations_comprehensive WHERE attended = 't')
    LEFT JOIN
      (SELECT mobilize_id::varchar,
              person_id FROM (SELECT DISTINCT user_id AS mobilize_id, user__email_address::varchar(256) AS email FROM mobilize.participations_comprehensive WHERE mobilize_id IS NOT NULL AND email IS NOT NULL)
       LEFT JOIN
       (SELECT * FROM (SELECT person_id, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY person_id NULLS LAST) AS dupe FROM bernie_data_commons.master_xwalk) WHERE dupe = 1 AND person_id IS NOT NULL) using(email)) 
      using(mobilize_id) 
      LEFT JOIN 
      (select * from akmobevents where mobilize_event_id is not null)
      using(mobilize_event_id))
    union all
    (select distinct person_id, event_recode from
      (SELECT DISTINCT user_id::varchar(256) AS actionkit_id ,
                       event_id::varchar(256) AS ak_event_id
       FROM ak_bernie.events_eventsignup WHERE attended = 't')
    LEFT JOIN
      (SELECT DISTINCT person_id::varchar,
                       actionkit_id::varchar
       FROM bernie_data_commons.master_xwalk_ak WHERE person_id IS NOT NULL) 
      using(actionkit_id)
    LEFT JOIN 
      (select * from akmobevents where ak_event_id is not null)
      using(ak_event_id))) where person_id is not null and event_recode is not null
    group by 1) akmob
    using(person_id) 

-- HOUSEHOLD LEVEL INFO
    LEFT JOIN 
    (SELECT 
        voting_address_id ,
        max(CASE WHEN ccj.person_id IS NOT NULL THEN 1 ELSE 0 END) AS id1_household ,
        max(CASE WHEN donors.user_id IS NOT NULL THEN 1 ELSE 0 END) AS donor_household ,
        max(CASE WHEN events.person_id IS NOT NULL AND (events.events_attended >= 1 OR events.events_rsvpd >= 1) THEN 1 ELSE 0 END) AS event_household ,
        max(CASE WHEN events.person_id IS NOT NULL AND events.volunteer >= 1 THEN 1 ELSE 0 END) AS volunteer_household
    FROM
        (SELECT person_id, voting_address_id FROM phoenix_analytics.person WHERE is_deceased = FALSE AND reg_voter_flag = TRUE) p
    JOIN bernie_data_commons.master_xwalk_dnc xw ON xw.person_id = p.person_id
    LEFT JOIN bernie_data_commons.ccj_dnc ccj ON ccj.person_id = p.person_id AND ccj.unique_id_flag = 1 AND ccj.support_int = 1
    LEFT JOIN
        (SELECT co.user_id AS user_id
        FROM ak_bernie.core_order co
        LEFT JOIN ak_bernie.core_transaction ct ON co.id = ct.order_id
        WHERE ct.success = 1 
                AND coalesce(ct.created_at, co.created_at, NULL) >= date('2019-02-18') 
                AND ct.type IN ('sale','credit') 
                AND ct.status IN ('completed','') 
                AND ct.amount > 0 
                AND co.status = 'completed'
        GROUP BY 1) donors ON donors.user_id = xw.actionkit_id
    LEFT JOIN
        (SELECT xw.person_id ,
                xw.actionkit_id ,
                xw.st_myc_van_id ,
                xw.jsonid_encoded ,
                pasr.state,
                count(CASE WHEN status_name IN ('Completed', 'Walk in') THEN pasr.event_id END) AS events_attended ,
                count(pasr.event_id) AS events_rsvpd ,
                COUNT(DISTINCT CASE WHEN pasr.role <> 'Attendee' THEN pasr.myc_van_id END) AS volunteer
        FROM ptg.ptg_all_shifts_regioned pasr
        JOIN bernie_data_commons.master_xwalk_st_myc xw ON xw.myc_van_id = pasr.myc_van_id AND xw.state_code = pasr.state
        WHERE status_rank = 1 AND event_date::date < (CURRENT_DATE)::date
        GROUP BY 1,2,3,4,5) events ON events.person_id = xw.person_id GROUP BY 1) p_household 
        ON p_household.voting_address_id = p.voting_address_id

-- CONSTITUENCIES
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
     (SELECT 
     	p.person_id::varchar,
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
        as20.civis_2020_partisanship::FLOAT8,
        as18.civis_2018_economic_persuasion::FLOAT8,
        as18.civis_2018_one_pct_persuasion::FLOAT8,
        as18.civis_2018_marijuana_persuasion::FLOAT8,
        as18.civis_2018_college_persuasion::FLOAT8,
        as18.civis_2018_welcome_persuasion::FLOAT8,
        as18.civis_2018_sexual_assault_persuasion::FLOAT8         

      FROM phoenix_analytics.person p
      LEFT JOIN phoenix_scores.all_scores_2020 as20 using(person_id)
      LEFT JOIN phoenix_scores.all_scores_2018 as18 using(person_id)
      LEFT JOIN bernie_data_commons.rainbow_analytics_frame rainbo using(person_id)) voterinfo 
     using(person_id)
    
  );

commit;
