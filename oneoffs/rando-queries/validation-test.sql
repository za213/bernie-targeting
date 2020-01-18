DROP TABLE IF EXISTS bernie_nmarchio2.test;
CREATE TABLE bernie_nmarchio2.test AS (
    select coalesce(a.person_id,b.person_id,c.person_id) as person_id
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
     from (
         SELECT person_id::varchar,
        state,
        campaign_id,
        campaign_type,
        contact_method,
        survey_date,
        support_int,
        first_choice
        FROM bernie_data_commons.third_party_ids
        WHERE person_id IS NOT NULL AND state IN ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA','ND','WA','MI','ID','MI','MO','AZ','OH','FL','IL')
        ORDER BY survey_date desc LIMIT 30000
        ) a
    full join (
        SELECT *
        FROM (
            SELECT ccj.person_id::varchar,
            ccj.contactdate as contactdate_field,
            ccj.support_id as support_id_field,
            ccj.support_int as support_int_field,
            ccj.voter_state as voter_state_field,
            row_number() over (partition BY ccj.voter_state ORDER BY contacttimestamp desc) AS state_stratification
            FROM bernie_data_commons.contactcontacts_joined ccj
            join phoenix_analytics.person p on p.person_id = ccj.person_id
            left join (
                select state_code
                ,avg(case when party_id is not null then 1 else 0 end) as party_reg_avg
                from phoenix_analytics.person
                where reg_voter_flag = true
                group by 1
            ) state_reg on state_reg.state_code = ccj.voter_state
            WHERE ccj.support_id IS NOT NULL
            AND ccj.person_id IS NOT NULL
            AND ccj.support_int between 1 and 5
            and ccj.unique_id_flag = true
            AND ccj.voter_state IN ('IA','NH','SC','NV','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA','ND','WA','MI','ID','MI','MO','AZ','OH','FL','IL')
            and (p.party_id in (1,7,8) or state_reg.party_reg_avg = 0)
        ) x
        WHERE state_stratification <= 5000
        ) b using(person_id)
    LEFT JOIN (
        SELECT person_id::varchar,
        voterbase_id,
        row_number() over (partition BY person_id ORDER BY voterbase_id NULLS LAST) AS dup
        FROM bernie_data_commons.master_xwalk
    ) c using(person_id)
);

;
/*-- check
select state_coalesced
,count(support_int) as third_party_ids
,count(support_id_field) as field_ids
,count(case when support_id_field = 1 then person_id end) as field_1s
,count(case when support_id_field = 5 then person_id end) as field_5s
from bernie_nmarchio2.test
group by 1
*/
;

DROP TABLE IF EXISTS bernie_nmarchio2.delivery;
CREATE TABLE bernie_nmarchio2.delivery AS
(select person_id, voterbase_id, coalesce(TO_DATE(contactdate_field, 'YYYY-MM-DD'), TO_DATE(survey_date, 'YYYY-MM-DD')) as date 
	from bernie_nmarchio2.test)

