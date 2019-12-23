SELECT cc.person_id::VARCHAR,
		max(CASE WHEN srt.surveyresponsetext = 'Volunteer Yes' THEN 1 ELSE 0 END) AS vol_yes
FROM contacts.contactscontact cc
JOIN contacts.surveyresponses sr ON cc.contactcontact_id = sr.contactcontact_id
JOIN contacts.surveyquestiontext sqt ON sqt.surveyquestionid = sr.surveyquestionid
JOIN contacts.surveyresponsetext srt ON srt.surveyresponseid = sr.surveyresponseid
WHERE sqt.surveyquestiontext ilike '%volunteer%'
GROUP BY 1;


select
person_id
,sum(case when 
activist_code_name ilike '%2016 Donor%' or
activist_code_name ilike '%2020 Donor%' or
activist_code_name ilike '%2020 Rec Donor%' or
activist_code_name ilike '%Low Dollar Donor%' then 1 else 0 end) as donor_myc

,sum(case when
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
activist_code_name ilike '%2016 Supporter%' then 1 else 0 end) as volunteer_prospect_myc

,sum(case when
activist_code_name ilike '%Labor%' or
activist_code_name ilike '%Labor Members%' or
activist_code_name ilike '%Labor CG Mem%' or
activist_code_name ilike '%Pol-Labor Endorsers%' or
activist_code_name ilike '%Pol-Labor%' then 1 else 0 end) as union_activist

,sum(case when
activist_code_name ilike '%16 GV RallyRSVP%' or
activist_code_name ilike '%16 CHS Rally RSVP%' or
activist_code_name ilike '%16 Col Rally RSVP%' or
activist_code_name ilike '%2016 Winthrop RSVP%' or
activist_code_name ilike '%2016 Benedict RSVP%' or
activist_code_name ilike '%2016 Florence RSVP%' or
activist_code_name ilike '%2016 Sumter Ral RSVP%' then 1 else 0 end) as rsvp_myc

,sum(case when
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
activist_code_name ilike '%Delegate%' or
activist_code_name ilike '%2016 County Delegate%' or
activist_code_name ilike '%2016 Canvass%' or
activist_code_name ilike '%2016 Canvass Captain%' or
activist_code_name ilike '%Crowd_Canvas_BERN%' or
activist_code_name ilike '%AK Com. Canvass Host%' or
activist_code_name ilike '%IA_Shift_6-16--July%' or
activist_code_name ilike '%2016 DVC Shift Compl%' or
activist_code_name ilike '%2016 Knock Doors%' or
activist_code_name ilike '%16Door Knock Sign Up%' or
activist_code_name ilike '%Give ride to caucus%' or
activist_code_name ilike '%Phone Bank%' or
activist_code_name ilike '%HQ_Phonebank_yes%' or
activist_code_name ilike '%2016 Phone Bank SU%' or
activist_code_name ilike '%VT_HQ_Phonebank%' then 1 else 0 end) as shiftvolunteer_myc

,sum(case when 
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
activist_code_name ilike '%High School Student%' then 1 else 0 end) as student_myc

,sum(case when activist_code_name ilike '%Teacher%' then 1 else 0 end) as teacher_myc

FROM phoenix_demssanders20_vansync_derived.activist_myc ac
JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id
LEFT JOIN bernie_data_commons.master_xwalk mx ON mx.myc_van_id = ac.myc_van_id AND mx.state_code = ac.state_code 
group by 1;
