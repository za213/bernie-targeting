drop table if exists bernie_nmarchio2.spoke_dvs;
create table bernie_nmarchio2.spoke_dvs 
distkey(person_id)
sortkey(person_id) as

(
select
person_id::varchar,
  
case when support_init = 1 then 1
else 0 end as spoke_support_1box,

case when support_change >= 1 then 1
when support_init = 1 then null
else 0 end as spoke_persuasion_1plus,

case when support_change <= -1  then 1
when support_init = 5 then null
else 0 end as spoke_persuasion_1minus,
  
case when support_change = 0  then 1
else 0 end as spoke_persuasion_nochange,
  
support_init,
support_final,
support_change 

from (select 
person_id,
sr.*, 
srt.*,
contactdate,
row_number() over (partition by person_id order by contactdate asc) as dup,
json_extract_path_text(lower(surveyresponseopen), 'support_init') as support_init,
json_extract_path_text(lower(surveyresponseopen), 'support_final') as support_final,
json_extract_path_text(lower(surveyresponseopen), 'support_change') as support_change 
 from (select * from contacts.surveyresponses where surveyquestionid = 28) sr 
join bernie_data_commons.contactcontacts_joined ccj using(contactcontact_id)
left join contacts.surveyresponsetext srt using(surveyresponseid)
where ccj.lalvoterid is not null)
where dup = 1 and person_id is not null);
