CREATE TABLE bernie_nmarchio2.vol_recruit_table
distkey(person_id) 
sortkey(person_id) as
(SELECT *
        ,NTILE(20) OVER (PARTITION BY state_code ORDER BY vol_recruit_rank ASC) AS vol_recruit_rank_20
 from
 (select  person_id
         ,state_code
         ,case 
         when ccj_id_1 = 1
         or bernapp = 1 
         or slack_vol = 1 
         or akmob_rsvps > 1 
         or akmob_attended > 0 
         or donor_1plus_flag > 0 
         or activist_flag = 1 
         or volunteer_shifts_myc = 1
         or mvp_myc = 1
         or activist_myc = 1
         or canvass_myc = 1
         or sticker_id = 1
         or commit2caucus_id = 1
         or event_signup_1plus_flag = 1 then '1 - Tier 1: Top Activists, Donors, Supporters'
         when all_flags_myc = 1
         or all_flags_surveys = 1 
         or student_flag = 1 
         or student_hash_flag = 1
         or akmob_rsvps > 0 
         or event_signup_0_flag = 1
         or event_signup_1_flag = 1
         or donor_0_flag = 1 then '2 - Tier 2: Activists, Donors, Supporters'
         when ccj_id_1_hh = 1 
         or activist_household_flag = 1
         or any_activist_donor_flag = 1
         or any_activist_donor_household_flag = 1
         or all_flags_myc_household_flag = 1
         or all_flags_surveys_household_flag = 1
         or akmob_rsvps_household_flag = 1
         or akmob_attended_household_flag = 1
         or bernapp_household_flag = 1
         or slack_vol_household_flag = 1
         or donor_1plus_household_flag = 1 then '3 - Tier 3: Households of Activists, Donors, Supporters'
         when attendee_100 >= 90
         or kickoff_party_rally_barnstorm_attendee_100 >= 90
         or canvasser_phonebank_attendee_100 >= 90
         or bernie_action_100 >= 90 then '4 - Tier 4: Inside Invite Guardrail'
         else '5 - Tier 5: Outside Invite Guardrail' end as vol_recruit_tiers
         ,row_number() OVER (PARTITION BY state_code  ORDER BY vol_recruit_tiers ASC, canvasser_phonebank_attendee DESC) as vol_recruit_rank
from bernie_data_commons.base_universe 
where civis_2020_partisanship >= .66 or party_8way = '1 - Democratic' 
or any_activist_donor_flag = 1 
and vol_recruit_tiers IN ('1 - Tier 1: Top Activists, Donors, Supporters', 
	                       '2 - Tier 2: Activists, Donors, Supporters',
	                       '3 - Tier 3: Households of Activists, Donors, Supporters', 
	                       '4 - Tier 4: Inside Invite Guardrail') ) );
