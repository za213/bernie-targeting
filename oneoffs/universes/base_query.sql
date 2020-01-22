
# https://github.com/Bernie-2020/universe_review/blob/master/GOTV%20Universes/gotv_person_flags.sql

(SELECT person_id,
    case when bern_flags = 1 then 100 else current_support_raw_100 end as support_targets_100,
    case when bern_flags = 1 then 20 else current_support_raw_20 end as support_targets_20,
    case when bern_flags = 1 then 10 else current_support_raw_10 end as support_targets_10,
    bern_flags,
    current_support_raw
FROM
(SELECT person_id::varchar,
       current_support_raw,
       coalesce(bern_flags,0) AS bern_flags,
       NTILE(100) OVER (PARTITION BY state_code||'_'||coalesce(bern_flags,0) ORDER BY current_support_raw DESC) AS current_support_raw_100,
       NTILE(20) OVER (PARTITION BY state_code||'_'||coalesce(bern_flags,0)ORDER BY current_support_raw DESC) AS current_support_raw_20,
       NTILE(10) OVER (PARTITION BY state_code||'_'||coalesce(bern_flags,0) ORDER BY current_support_raw DESC) AS current_support_raw_10
FROM bernie_data_commons.all_scores
LEFT JOIN
  (SELECT person_id::varchar,
          CASE
              WHEN actionkit_id IS NOT NULL
                   OR st_myc_van_id IS NOT NULL
                   OR student_hash IS NOT NULL
                   OR field_support_int = 1
                   OR f_ctc_dem = 1
                   OR f_ctc_npp = 1
                   OR f_ctc_other_party = 1
                   OR field_support_int = 1
                   OR f_donor_in_household = 1
                   OR f_event_rsvp_in_household = 1
                   OR f_event_attendee_in_household = 1
                   OR f_in_student_roster_reg = 1
                   OR f_in_student_roster_unreg = 1 THEN 1 ELSE 0
          END AS bern_flags
   FROM gotv_universes.gotv_person_flags) using(person_id)))
