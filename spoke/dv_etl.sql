drop table if exists modeling.spoke_dvs;
create table modeling.spoke_dvs 
distkey(person_id)
sortkey(person_id) as
(
select
person_id::varchar,
case when support_init = 1 then 1
else 0 end as spoke_support_1box,

case when support_change >= 1 then 1
else 0 end as spoke_persuasion_1plus,

case when support_change >= 2 then 1
else 0 end as spoke_persuasion_2plus

from (select 
person_id,
sr.*, 
srt.*,
contactdate,
row_number() over (partition by person_id order by contactdate asc) as dup,
json_extract_path_text(lower(surveyresponseopen), 'support_init') as support_init,
json_extract_path_text(lower(surveyresponseopen), 'support_final') as support_final,
json_extract_path_text(lower(surveyresponseopen), 'support_change') as support_change 
 from bernie_data_commons.contactcontacts_joined ccj
    left join contacts.surveyresponses sr using(contactcontact_id)
left join contacts.surveyresponsetext srt using(surveyresponseid)
where sr.surveyquestionid = 28 and ccj.lalvoterid is not null)
where dup = 1 and person_id is not null);
