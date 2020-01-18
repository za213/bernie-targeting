DROP TABLE IF EXISTS bernie_nmarchio2.civis_random_sample;
CREATE TABLE bernie_nmarchio2.civis_random_sample AS
(select 
coalesce(a.person_id,b.person_id,c.person_id) as person_id
,coalesce(voter_state_field,state) as state_coalesced
,voterbase_id
,state
,campaign_id
,campaign_type
,contact_method
,survey_date
,support_int
,first_choice
,contactdate_field
,support_id_field
,support_int_field
,voter_state_field
 from 
(SELECT person_id::varchar,
        state,
        campaign_id,
        campaign_type,
        contact_method,
        survey_date,
        support_int,
        first_choice
        FROM bernie_data_commons.third_party_ids 
 WHERE person_id IS NOT NULL AND state IN ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA') 
 ORDER BY survey_date desc LIMIT 30000) a
full join
(SELECT *
FROM (
SELECT person_id::varchar,
	   contactdate as contactdate_field,
       support_id as support_id_field,
       support_int as support_int_field,
       voter_state as voter_state_field,
       row_number() over (partition BY voter_state ORDER BY contactdate desc) AS state_stratification
FROM bernie_data_commons.contactcontacts_joined
WHERE support_id IS NOT NULL
  AND person_id IS NOT NULL
  AND support_int between 1 and 5
  AND voter_state IN ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA'))
WHERE state_stratification <= 5000) b 
using(person_id)
LEFT JOIN
(SELECT person_id::varchar,
        voterbase_id,
        row_number() over (partition BY person_id ORDER BY voterbase_id NULLS LAST) AS dup
FROM bernie_data_commons.master_xwalk) c
using(person_id)
left join
(select person_id, party_dem, party_independent, non_party_reg_state  from bernie_data_commons.rainbow_modeling_frame)
using(person_id)
where (party_dem = 1 or party_independent = 1 or non_party_reg_state = 1)
);

DROP TABLE IF EXISTS bernie_nmarchio2.delivery;
CREATE TABLE bernie_nmarchio2.delivery AS
(select person_id, voterbase_id, coalesce(TO_DATE(contactdate_field, 'YYYY-MM-DD'), TO_DATE(survey_date, 'YYYY-MM-DD')) as date 
	from bernie_nmarchio2.test)



