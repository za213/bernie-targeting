DROP TABLE IF EXISTS bernie_nmarchio2.support_id_recode;
CREATE TABLE bernie_nmarchio2.support_id_recode 
  DISTSTYLE KEY
  DISTKEY (person_id)
  SORTKEY (person_id)
AS 
(SELECT person_id::varchar(10) ,
       CASE
           WHEN support_int = 1 THEN 1
           WHEN support_int IS NOT NULL
                OR support_int <> 0
                OR first_choice <> 'Donald Trump' THEN 0
           ELSE NULL
       END AS support_1_id
FROM bernie_data_commons.third_party_ids
INNER JOIN
  (SELECT person_id,
          CASE
              WHEN (f_ctc_dem = 1
                    OR f_ctc_last_60_days = 1
                    OR f_ctc_npp = 1
                    OR f_donated = 1
                    OR f_ctc_other_party = 1
                    OR f_core_donut_top50 = 1
                    OR f_hosted_1_event = 1
                    OR f_id_1_other_party = 1
                    OR f_id_1_dem = 1
                    OR f_id_1_last_60_days = 1
                    OR f_id_1_npp = 1
                    OR f_support_bucket_floor_80 = 1
                    OR f_support_bucket_floor_90 = 1) THEN 1
              ELSE 0
          END AS in_core ,
          CASE
              WHEN f_donut_anti_bernie =1
                   OR field_support_int IN (4,
                                            5) THEN 1
              ELSE 0
          END AS in_exclusion
   FROM gotv_universes.gotv_person_flags
   WHERE state_code = 'IA' AND person_id IS NOT NULL) using(person_id)
WHERE in_core = 0
  AND in_exclusion = 0
  AND state = 'IA'
  AND datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), TO_DATE('2020-01-10','YYYY-MM-DD')) <= 90);
