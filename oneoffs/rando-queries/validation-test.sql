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
 ORDER BY random() LIMIT 50000) a
full join
(SELECT *
FROM (
SELECT person_id::varchar,
	   contactdate as contactdate_field,
       support_id as support_id_field,
       support_int as support_int_field,
       voter_state as voter_state_field,
       row_number() over (partition BY voter_state ORDER BY random()) AS state_stratification
FROM bernie_data_commons.contactcontacts_joined
WHERE support_id IS NOT NULL
  AND person_id IS NOT NULL
  AND voter_state IN ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA'))
WHERE state_stratification <= 10000) b 
using(person_id)
LEFT JOIN
(SELECT person_id::varchar,
        voterbase_id,
        row_number() over (partition BY person_id ORDER BY voterbase_id NULLS LAST) AS dup
FROM bernie_data_commons.master_xwalk) c
using(person_id));
