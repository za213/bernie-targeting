(select
person_id::varchar(10) 
,       CASE
           WHEN third_party_support_int = 1 THEN 1
           WHEN third_party_support_int IS NOT NULL
                OR third_party_support_int <> 0
                OR third_party_first_choice <> 'Donald Trump' THEN 0
           ELSE NULL
       END AS support_1_id
,third_party_support_int
,third_party_survey_date
,CASE
    WHEN (f_ctc_dem = 1
        OR f_ctc_last_60_days = 1
        OR f_ctc_npp = 1
        OR f_ctc_other_party = 1
        OR f_donated = 1
        OR f_core_donut_top50 = 1
        OR f_hosted_1_event = 1
        OR f_support_bucket_floor_80 = 1
        OR f_support_bucket_floor_90 = 1
    ) THEN 1
    ELSE 0
    END AS in_core_excl_1s
,CASE
    WHEN f_donut_anti_bernie =1
    THEN 1
    ELSE 0
    END AS in_exclusion
from 
gotv_universes.gotv_person_flags
where third_party_survey_date between '2019-12-20' and '2020-01-10' 
and state_code = 'IA'
and person_id is not null
) where and in_core_excl_1s = 0 and in_exclusion = 0 
