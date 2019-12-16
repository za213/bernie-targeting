
DROP TABLE IF EXISTS bernie_nmarchio2.iowa_volunteer_dvs;
CREATE TABLE bernie_nmarchio2.iowa_volunteer_dvs distkey (person_id) sortkey (person_id) AS (
				SELECT person_id,
		CASE WHEN rsvp_count >= 1 THEN 1 ELSE 0 END AS rsvp_1plus,
		CASE WHEN rsvp_count = 1 AND shift_count = 0 THEN 1 ELSE 0 END AS flaked_on_1plus_rsvps,
		CASE WHEN shift_count >= 1 THEN 1 ELSE 0 END AS shift_1plus FROM (
		SELECT person_id,
			sum(recruit_status) AS rsvp_count,
			sum(complete_status) AS shift_count
		FROM (
			SELECT handdials.myc_van_id,
				handdials.person_id,
				recruit_date,
				--case when result_name = 'Canvassed' then 1 else 0 end contact_status,
				CASE WHEN es.myc_van_id IS NOT NULL AND result_name = 'Canvassed' THEN 1 ELSE 0 END AS "recruit_status",
				CASE WHEN es.myc_van_id IS NOT NULL AND result_name = 'Canvassed' AND current_status = 'Completed' THEN 1 ELSE 0 END AS "complete_status"
			FROM     (SELECT c.datetime_created - interval '5 hours' AS "timestamp_c",
                        DATE (date_trunc('week', timestamp_c)) AS "week_of_contact",
                        DATE (timestamp_c) recruit_date,
                        coalesce(c.person_id, x.person_id) person_id,
                           c.myc_van_id,
                           result_name
                    FROM phoenix_demssanders20_vansync_derived.contacts_myc c
                    left join bernie_data_commons.master_xwalk_st_myv x on 'IA-' || c.myv_van_id = x.st_myv_van_id
                    WHERE contact_type_name = 'Phone'
                        AND c.state_code = 'IA'
                        AND
                        --exclude political department users
                        canvassed_by_user_id NOT IN (1737931, 1738590, 1737929, 1811727, 1808647, 1799908)
                        AND committee_id = 73296
                    ) handdials
			LEFT JOIN (
				SELECT DISTINCT orig.*
				FROM (
					SELECT signups.myc_van_id,
						events.date_offset_begin event_date,
						events.event_name event_name,
						events.event_id event_id,
						event_types.event_type_name event_type,
						signups.event_signup_id AS event_signup_id,
						signups.event_shift_id,
						signups_statuses.datetime_modified - interval '5 hours' shift_modified,
						DATE (shift_modified) date_modified,
						signups_statuses_ref.event_status_name status_name,
						signups.state_code STATE,
						status_ref2.event_status_name current_status,
						signups.current_event_signups_event_status_id,
						RANK() OVER (
							PARTITION BY signups.event_signup_id ORDER BY signups_statuses.datetime_modified
							) status_rank_asc
					FROM phoenix_demssanders20_vansync.event_signups signups
					JOIN (
						SELECT state_code,
							event_signup_id,
							event_status_id,
							datetime_modified
						FROM phoenix_demssanders20_vansync.event_signups_statuses
						WHERE created_by_committee_id = 73296
							AND state_code = 'IA'
						GROUP BY 1,
							2,
							3,
							4
						) signups_statuses ON signups.event_signup_id = signups_statuses.event_signup_id
						AND signups.state_code = signups_statuses.state_code
					-- select the actual status using the time of the most recent status per type above
					JOIN phoenix_vansync_ref.event_statuses signups_statuses_ref ON signups_statuses.event_status_id = signups_statuses_ref.event_status_id
					JOIN (
						SELECT event_signups_event_status_id,
							event_status_id
						FROM phoenix_demssanders20_vansync.event_signups_statuses
						WHERE created_by_committee_id = 73296
							AND state_code = 'IA'
						) cur ON signups.current_event_signups_event_status_id = cur.event_signups_event_status_id
					JOIN phoenix_vansync_ref.event_statuses status_ref2 ON cur.event_status_id = status_ref2.event_status_id
					-- get event information for event type
					JOIN phoenix_demssanders20_vansync.events EVENTS ON signups.event_id = events.event_id
						AND signups.state_code = events.state_code
						AND events.created_by_committee_id = 73296
					INNER JOIN phoenix_demssanders20_vansync.event_types event_types ON events.event_type_id = event_types.event_type_id
					WHERE signups.state_code = 'IA'
						AND signups.created_by_committee_id = 73296
					) orig
				WHERE status_rank_asc = 1
				) es ON handdials.myc_van_id = es.myc_van_id
				AND handdials.recruit_date = es.date_modified
				AND status_rank_asc = 1
				AND status_name = 'Scheduled'
			--limit to contact_status = 1 to filter to people who answered phone
			WHERE result_name = 'Canvassed'
			)
		where person_id is not null GROUP BY 1
		)
		
		);
