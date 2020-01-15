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
WHERE STATE = 'IA'
  AND datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), TO_DATE('2020-01-10','YYYY-MM-DD')) <= 60
  AND person_id IN
    (SELECT person_id
     FROM gotv_universes.gotv_person_flags
     WHERE state_code = 'IA'
       AND (f_ctc_dem = 1
            OR f_ctc_last_60_days = 1
            OR f_ctc_npp = 1
            OR f_ctc_other_party = 1
            OR f_id_1_other_party = 1
            OR f_donated = 1
            OR f_core_donut_top50 = 1
            OR f_hosted_1_event = 1
            OR f_id_1_dem = 1
            OR f_id_1_last_60_days = 1
            OR f_id_1_npp = 1
            OR f_support_bucket_floor_80 = 1
            OR f_support_bucket_floor_90 = 1)
      AND ((f_donut_anti_bernie =0 or f_donut_anti_bernie is null)
           AND (field_support_int NOT IN (4,5) or field_support_int is null)))
            );
