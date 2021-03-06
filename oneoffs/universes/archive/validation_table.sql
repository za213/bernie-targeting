
DROP TABLE IF EXISTS bernie_nmarchio2.gotv_validation_check;
CREATE TABLE bernie_nmarchio2.gotv_validation_check
distkey(person_id) 
sortkey(person_id) AS
(select * from 
-- Person table restricted to states and valid universe
(SELECT person_id::varchar,
  state_code
  FROM phoenix_analytics.person
  WHERE is_deceased = FALSE
    AND reg_record_merged = FALSE
    AND reg_on_current_file = TRUE
    AND reg_voter_flag = TRUE 
    AND state_code IN ('MN', 'MI', 'ME', 'MS', 'VA')) 
LEFT JOIN
-- All Scores
(SELECT
person_id::varchar,
NTILE(10) OVER (PARTITION BY state_code ORDER BY current_support_raw ASC) AS current_support_raw_10,
NTILE(10) OVER (PARTITION BY state_code ORDER BY field_id_1_score ASC) AS field_id_1_score_10
FROM bernie_data_commons.all_scores)
using(person_id)

LEFT JOIN
-- Third Party 
(select * from (select *
,case 
when validtime1 = 1 THEN 1
WHEN holdout1 = 1 THEN 1
ELSE 0 END AS validation_id1
 from (select 
 person_id::varchar(10)
 ,case 
WHEN datediff(d, '2020-01-17', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state = 'MN' THEN 1 
WHEN datediff(d, '2020-01-22', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state IN ('TN','AR','MO','AL','KY','GA','MS') THEN 1
WHEN datediff(d, '2019-12-01', TO_DATE(survey_date, 'YYYY-MM-DD')) > 0 and state NOT IN ('MN','TN','AR','MO','AL','KY','GA','MS','NH','NV','IA','SC') THEN 1
ELSE 0 END as validtime1
-- Candidate Support DVs
,case when first_choice = 'Bernie Sanders' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_bernie
,case when first_choice = 'Donald Trump' then 1 when first_choice is not null then 0 else NULL end as first_choice_trump
,case when first_choice = 'Joe Biden' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_biden
,case when first_choice = 'Elizabeth Warren' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_warren
,case when first_choice = 'Pete Buttigieg' then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_buttigieg
,case when first_choice IS NOT NULL then 1 when first_choice is not null or first_choice <> 'Donald Trump' then 0 else NULL end as first_choice_any
-- Support ID DVs
,case when support_int = 1 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_1_id
,case when support_int = 2 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_2_id
,case when support_int IN (1,2) then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_1_2_id
,case when support_int = 3 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_3_id
,case when support_int = 4 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_4_id
,case when support_int = 5 then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_5_id
,case when support_int IN (1,2,3,4,5) then 1 when support_int is not null or support_int <> 0 or first_choice <> 'Donald Trump' then 0 else NULL end as support_1_2_3_4_5_id

FROM bernie_data_commons.third_party_ids) third
LEFT JOIN
(select person_id::varchar as pid1, 1 as holdout1 from haystaq.set_no_current where set_no=3) ho 
on third.person_id = ho.pid1) where validation_id1 =1)
using(person_id)

LEFT JOIN 
-- Field IDs
(select * from 
(select *,
        case 
        WHEN  validtime2 = 1 then 1
        WHEN holdout2 = 1 THEN 1
        ELSE 0 END AS validation_id2
FROM 
(SELECT person_id::varchar , 
        CASE WHEN resultcode IN ('Canvassed', 'Do Not Contact', 'Refused', 'Call Back', 'Language Barrier', 'Hostile', 'Come Back', 'Cultivation', 'Refused Contact', 'Spanish', 'Other', 'Not Interested') THEN 1 ELSE 0 END AS contact_made , 
        CASE WHEN resultcode IN ('Do Not Contact','Hostile','Refused','Refused Contact') OR (support_int = 4 OR support_int = 5) THEN 1 ELSE 0 END AS negative_result ,
        case 
        WHEN datediff(d, '2020-01-17', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state = 'MN' THEN 1 
        WHEN datediff(d, '2020-01-22', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state IN ('TN','AR','MO','AL','KY','GA','MS') THEN 1
        WHEN datediff(d, '2019-12-01', TO_DATE(contactdate, 'YYYY-MM-DD')) > 0 and voter_state NOT IN ('MN','TN','AR','MO','AL','KY','GA','MS','NH','NV','IA','SC') THEN 1
        ELSE 0 END as validtime2, 
        COUNT(DISTINCT CASE WHEN support_int = 1 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_1 ,
        COUNT(DISTINCT CASE WHEN support_int IN (1,2) AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_1_2 ,
        COUNT(DISTINCT CASE WHEN support_int = 2 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_2 ,
        COUNT(DISTINCT CASE WHEN support_int = 3 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_3 ,
        COUNT(DISTINCT CASE WHEN support_int = 4 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_4 ,
        COUNT(DISTINCT CASE WHEN support_int = 5 AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_5 ,
        COUNT(DISTINCT CASE WHEN support_int IN (1,2,3,4,5) AND unique_id_flag=TRUE THEN person_id END) AS ccj_id_1_2_3_4_5
        FROM bernie_data_commons.ccj_dnc 
        WHERE unique_id_flag = 1 AND person_id IS NOT NULL GROUP BY 1,2,3,4) ccj
        LEFT JOIN 
        (select person_id::varchar as pid2, 1 as holdout2 from haystaq.set_no_current where set_no=3) ho2
        on ccj.person_id = ho2.pid2) where validation_id2 = 1)
using(person_id)

LEFT JOIN
-- Universes
(select person_id::varchar, gotv_univ_state from
(select person_id::varchar, 'MN Universe' as gotv_univ_state from gotv_universes.mn_gotv_universe_20200123)
UNION ALL
(select person_id::varchar, 'MI Universe' as gotv_univ_state from gotv_universes.mi_gotv_universe_20200124)
UNION ALL
(select person_id::varchar, 'ME Universe' as gotv_univ_state from gotv_universes.ME_gotv_universe_20200124)
UNION ALL
(select person_id::varchar, 'MS Universe' as gotv_univ_state from gotv_universes.MS_gotv_universe_20200126)
UNION ALL
(select person_id::varchar, 'VA Universe' as gotv_univ_state from gotv_universes.VA_gotv_universe_20200124))
using(person_id)
where coalesce(validtime1::varchar, first_choice_bernie::varchar, first_choice_trump::varchar, first_choice_biden::varchar, first_choice_warren::varchar, first_choice_buttigieg::varchar, support_1_id::varchar, support_2_id::varchar, support_3_id::varchar, support_4_id::varchar, support_5_id::varchar, support_1_2_id::varchar, support_1_2_3_4_5_id::varchar, pid1::varchar, holdout1::varchar, validation_id1::varchar, contact_made::varchar, negative_result::varchar, validtime2::varchar, ccj_id_1::varchar, ccj_id_1_2::varchar, ccj_id_2::varchar, ccj_id_3::varchar, ccj_id_4::varchar, ccj_id_5::varchar, ccj_id_1_2_3_4_5::varchar, pid2::varchar, holdout2::varchar, validation_id2::varchar, gotv_univ_state::varchar) is not null
);


