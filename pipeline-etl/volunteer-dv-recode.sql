CREATE TEMP TABLE volunteer_temp_table AS (
	SELECT person_id,
	coalesce(activist_codes.donor, 0) donor,
	coalesce(activist_codes.email_signup, 0) email_signup,
	coalesce(activist_codes.event_signup, 0) event_signup,
	coalesce(activist_codes.text_signup, 0) text_signup,
	coalesce(activist_codes.barnstorm, 0) barnstorm,
	coalesce(activist_codes.rally, 0) rally,
	coalesce(activist_codes.event_or_barnstorm_or_rally, 0) event_or_barnstorm_or_rally,
	coalesce(activist_codes.bern_user, 0) bern_user,
	coalesce(activist_codes.supporter_16, 0) supporter_16,
	coalesce(activist_codes.delegate_16, 0) delegate_16,
	coalesce(response.vol_yes, 0) vol_yes FROM (
	SELECT cc.person_id::VARCHAR,
		max(CASE WHEN srt.surveyresponsetext = 'Volunteer Yes' THEN 1 ELSE 0 END) AS vol_yes
	FROM contacts.contactscontact cc
	JOIN contacts.surveyresponses sr ON cc.contactcontact_id = sr.contactcontact_id
	JOIN contacts.surveyquestiontext sqt ON sqt.surveyquestionid = sr.surveyquestionid
	JOIN contacts.surveyresponsetext srt ON srt.surveyresponseid = sr.surveyresponseid
	WHERE sqt.surveyquestiontext ilike '%volunteer%'
	GROUP BY 1
	) response LEFT JOIN (
	SELECT mx.person_id::VARCHAR,
		ac.state_code,
		ac.myc_van_id,
		max(CASE WHEN ac_lookup.activist_code_name = '2016 Donor'
					OR ac_lookup.activist_code_name = '2020 Donor'
					OR ac_lookup.activist_code_name = '2020 Rec Donor'
					THEN 1 ELSE 0 END) AS donor,
		max(CASE WHEN ac_lookup.activist_code_name = 'AK Email Signup' THEN 1 ELSE 0 END) AS email_signup,
		max(CASE WHEN ac_lookup.activist_code_name = 'AK Event Sign-up' THEN 1 ELSE 0 END) AS event_signup,
		max(CASE WHEN ac_lookup.activist_code_name = 'AK Text Signup' THEN 1 ELSE 0 END) AS text_signup,
		max(CASE WHEN ac_lookup.activist_code_name = 'AK Barnstorm Signup' THEN 1 ELSE 0 END) AS barnstorm,
		max(CASE WHEN ac_lookup.activist_code_name = 'AK Rally Signup' THEN 1 ELSE 0 END) AS rally,
		max(CASE WHEN ac_lookup.activist_code_name = 'AK Barnstorm Signup'
					OR ac_lookup.activist_code_name = 'AK Rally Signup'
					OR ac_lookup.activist_code_name = 'AK Event Sign-up' 
          THEN 1 ELSE 0 END) AS event_or_barnstorm_or_rally,
		max(CASE WHEN ac_lookup.activist_code_name = 'BERN User' THEN 1 ELSE 0 END) AS bern_user,
		max(CASE WHEN ac_lookup.activist_code_name = '2016 Supporter' THEN 1 ELSE 0 END) AS supporter_16,
		max(CASE WHEN ac_lookup.activist_code_name = '2016 County Delegate' 
      OR ac_lookup.activist_code_name = '2016 State Delegate' THEN 1 ELSE 0 END) AS delegate_16
	FROM phoenix_demssanders20_vansync_derived.activist_myc ac
	JOIN phoenix_demssanders20_vansync.activist_codes ac_lookup ON ac_lookup.activist_code_id = ac.activist_code_id
	LEFT JOIN bernie_data_commons.master_xwalk mx ON mx.myc_van_id = ac.myc_van_id
		AND mx.state_code = ac.state_code
	GROUP BY 1, 2, 3
	) activist_codes using (person_id) WHERE person_id IS NOT NULL
	);

DROP TABLE IF EXISTS bernie_nmarchio2.vol_refresh_dvs;
	CREATE TABLE bernie_nmarchio2.vol_refresh_dvs AS (
		SELECT * FROM 
    (SELECT * FROM volunteer_temp_table)
    UNION ALL
    (SELECT person_id,
				0 AS donor,
				0 AS email_signup,
				0 AS event_signup,
				0 AS text_signup,
				0 AS barnstorm,
				0 AS rally,
				0 AS event_or_barnstorm_or_rally,
				0 AS bern_user,
				0 AS supporter_16,
				0 AS delegate_16,
				0 AS vol_yes
			FROM phoenix_analytics.person
			WHERE is_deceased = false
				AND reg_record_merged = false
				AND reg_on_current_file = true
				AND reg_voter_flag = true
				AND person_id NOT IN (
					SELECT DISTINCT person_id volunteer_temp_table
					)
				AND random() < .1 limit 100
			)
		);
