
DROP TABLE IF EXISTS bernie_nmarchio2.support_id_recode;
CREATE TABLE bernie_nmarchio2.support_id_recode AS 
(select 
 person_id
-- Candidate Support DVs
,case when first_choice = 'Bernie Sanders' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_bernie
,case when first_choice = 'Donald Trump' then 1 when first_choice is not null then 0 else NULL end as first_choice_trump
,case when first_choice = 'Joe Biden' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_biden
,case when first_choice = 'Elizabeth Warren' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_warren
,case when first_choice = 'Pete Buttigieg' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_buttigieg
,case when first_choice = 'Kamala Harris' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_harris
,case when first_choice IN ('Julian Castro','Andrew Yang','Tulsi Gabbard','Cory Booker','Beto ORourke','Michael Bennett','Deval Patrick','Mike Bloomberg','Amy Klobuchar','Tom Steyer','Other','Someone else') then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_other_candidate
,case when first_choice IN ('Dont Know') then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_dont_know
-- Support ID DVs
,case when support_int = 1 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_1_id
,case when support_int = 2 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_2_id
,case when support_int = 3 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_3_id
,case when support_int = 4 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_4_id
,case when support_int = 5 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_5_id
,case when support_int IN (1,2) then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_1_2_id
,case when support_int IN (4,5) then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_4_5_id
-- Time dummies
,case when datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE) <= 30 then 1 else 0 end as time_dummy_last_30_days
,case when datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE) between 31 and 60 then 1 else 0 end as time_dummy_last_30_60_days
,case when datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE) between 61 and 90 then 1 else 0 end as time_dummy_last_30_90_days
,case when datediff(d, TO_DATE(survey_date, 'YYYY-MM-DD'), CURRENT_DATE) >= 91 then 1 else 0 end as time_dummy_over_90_days
-- Survey Mode
,case when contact_method IN ('SMS','Text','SMS2') then 1 else 0 end as mode_sms
,case when contact_method = 'IVR' then 1 else 0 end as mode_ivr
,case when contact_method IN ('LIVE','Live') then 1 else 0 end as mode_live
,case when contact_method = 'panel' then 1 else 0 end as mode_panel
from bernie_data_commons.third_party_ids);

