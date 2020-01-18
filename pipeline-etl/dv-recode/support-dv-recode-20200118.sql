
DROP TABLE IF EXISTS bernie_nmarchio2.iowa_run_dvs;
CREATE TABLE bernie_nmarchio2.iowa_run_dvs
  DISTSTYLE KEY
  DISTKEY (person_id)
  SORTKEY (person_id)
AS (SELECT * FROM
(SELECT
 person_id::varchar(10)

--Iowa, no IVR, last 90 days
,CASE
	WHEN contact_method = 'IVR' or state <> 'IA' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 90 then NULL
    WHEN first_choice = 'Bernie Sanders' THEN 1
    WHEN (first_choice IS NOT NULL OR first_choice <> 'Donald Trump') THEN 0
    ELSE NULL
END AS bernie_first_ia_noivr_last90 
,CASE
	WHEN contact_method = 'IVR' or state <> 'IA' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 90 then NULL
    WHEN support_int = 1  THEN 1
    WHEN support_int IS NOT NULL OR support_int <> 0 OR first_choice <> 'Donald Trump' THEN 0
    ELSE NULL
END AS support_1_id_ia_noivr_last90

--Iowa, all modes, last 60 days
,CASE
	WHEN state <> 'IA' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 60 then NULL
    WHEN first_choice = 'Bernie Sanders' THEN 1
    WHEN (first_choice IS NOT NULL OR first_choice <> 'Donald Trump') THEN 0
    ELSE NULL
END AS bernie_first_ia_last60 
,CASE
	WHEN state <> 'IA' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 60 then NULL
    WHEN support_int = 1  THEN 1
    WHEN support_int IS NOT NULL OR support_int <> 0 OR first_choice <> 'Donald Trump' THEN 0
    ELSE NULL
END AS support_1_id_ia_last60

--Iowa, all modes, last 30 days
,CASE
	WHEN state <> 'IA' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 30 then NULL
    WHEN first_choice = 'Bernie Sanders' THEN 1
    WHEN (first_choice IS NOT NULL OR first_choice <> 'Donald Trump') THEN 0
    ELSE NULL
END AS bernie_first_ia_last30 
,CASE
	WHEN state <> 'IA' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 30 then NULL
    WHEN support_int = 1  THEN 1
    WHEN support_int IS NOT NULL OR support_int <> 0 OR first_choice <> 'Donald Trump' THEN 0
    ELSE NULL
END AS support_1_id_ia_last30

--National, no IVR, last 60 days
,CASE
	WHEN contact_method = 'IVR' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 90 then NULL
    WHEN first_choice = 'Bernie Sanders' THEN 1
    WHEN (first_choice IS NOT NULL OR first_choice <> 'Donald Trump') THEN 0
    ELSE NULL
END AS bernie_first_f4st_noivr_last90 
,CASE
	WHEN contact_method = 'IVR' or datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 90 then NULL
    WHEN support_int = 1  THEN 1
    WHEN support_int IS NOT NULL OR support_int <> 0 OR first_choice <> 'Donald Trump' THEN 0
    ELSE NULL
END AS support_1_id_f4st_noivr_last90

--National, all modes, last 60 days
,CASE
	WHEN datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 60 then NULL
    WHEN first_choice = 'Bernie Sanders' THEN 1
    WHEN (first_choice IS NOT NULL OR first_choice <> 'Donald Trump') THEN 0
    ELSE NULL
END AS bernie_first_f4st_last60 
,CASE
	WHEN datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 60 then NULL
    WHEN support_int = 1  THEN 1
    WHEN support_int IS NOT NULL OR support_int <> 0 OR first_choice <> 'Donald Trump' THEN 0
    ELSE NULL
END AS support_1_id_f4st_last60

--National, all modes, last 30 days
,CASE
	WHEN datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 30 then NULL
    WHEN first_choice = 'Bernie Sanders' THEN 1
    WHEN (first_choice IS NOT NULL OR first_choice <> 'Donald Trump') THEN 0
    ELSE NULL
END AS bernie_first_f4st_last30 
,CASE
	WHEN datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE)  > 30 then NULL
    WHEN support_int = 1  THEN 1
    WHEN support_int IS NOT NULL OR support_int <> 0 OR first_choice <> 'Donald Trump' THEN 0
    ELSE NULL
END AS support_1_id_f4st_last30

from bernie_data_commons.third_party_ids)
where 
coalesce( bernie_first_ia_noivr_last90, 
 support_1_id_ia_noivr_last90,
 bernie_first_ia_last60 ,
 support_1_id_ia_last60,
 bernie_first_ia_last30,
 support_1_id_ia_last30,
 bernie_first_f4st_last30,
 support_1_id_f4st_last30,
 bernie_first_f4st_noivr_last90 ,
 support_1_id_f4st_noivr_last90,
 bernie_first_f4st_last60 ,
 support_1_id_f4st_last60) is not null);


