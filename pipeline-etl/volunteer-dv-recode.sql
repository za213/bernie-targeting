DROP TABLE IF EXISTS bernie_nmarchio2.vol_refresh_dvs;

CREATE TABLE bernie_nmarchio2.vol_refresh_dvs AS
(
select 
person_id,
coalesce(activist_codes.donor, 0)  donor,
coalesce(activist_codes.email_signup, 0) email_signup,
coalesce(activist_codes.event_signup, 0) event_signup,
coalesce(activist_codes.text_signup, 0) text_signup,
coalesce(activist_codes.barnstorm, 0) barnstorm,
coalesce(activist_codes.rally, 0) rally,
coalesce(activist_codes.event_or_barnstorm_or_rally, 0) event_or_barnstorm_or_rally,
coalesce(activist_codes.bern_user, 0) bern_user,
coalesce(activist_codes.supporter_16, 0) supporter_16,
coalesce(activist_codes.delegate_16, 0) delegate_16,
coalesce(response.vol_yes, 0) vol_yes

from 
(select cc.person_id::varchar, 
max(case when srt.surveyresponsetext='Volunteer Yes' then 1 else 0 end) as vol_yes 
from contacts.contactscontact cc 
join contacts.surveyresponses sr on cc.contactcontact_id = sr.contactcontact_id 
join contacts.surveyquestiontext sqt on sqt.surveyquestionid = sr.surveyquestionid 
join contacts.surveyresponsetext srt on srt.surveyresponseid = sr.surveyresponseid
where sqt.surveyquestiontext ilike '%volunteer%' group by 1) response
left join
(select
mx.person_id::varchar,
ac.state_code,
ac.myc_van_id,
max(case when ac_lookup.activist_code_name = '2016 Donor'
or ac_lookup.activist_code_name = '2020 Donor'
or ac_lookup.activist_code_name = '2020 Rec Donor'
then 1 else 0 end) as donor,
max(case when ac_lookup.activist_code_name = 'AK Email Signup'
then 1 else 0 end) as email_signup,
max(case when ac_lookup.activist_code_name = 'AK Event Sign-up'
then 1 else 0 end) as event_signup,
max(case when ac_lookup.activist_code_name = 'AK Text Signup'
then 1 else 0 end) as text_signup,
max(case when ac_lookup.activist_code_name = 'AK Barnstorm Signup'
then 1 else 0 end) as barnstorm,
max(case when ac_lookup.activist_code_name = 'AK Rally Signup'
then 1 else 0 end) as rally,
max(case when ac_lookup.activist_code_name = 'AK Barnstorm Signup'
or ac_lookup.activist_code_name = 'AK Rally Signup'
or ac_lookup.activist_code_name = 'AK Event Sign-up'
then 1 else 0 end) as event_or_barnstorm_or_rally,
max(case when ac_lookup.activist_code_name = 'BERN User'
then 1 else 0 end) as bern_user,
max(case when ac_lookup.activist_code_name = '2016 Supporter'
then 1 else 0 end) as supporter_16,
max(case when ac_lookup.activist_code_name =  '2016 County Delegate'
or ac_lookup.activist_code_name =  '2016 State Delegate'
then 1 else 0 end) as delegate_16
from
phoenix_demssanders20_vansync_derived.activist_myc ac 
join
phoenix_demssanders20_vansync.activist_codes ac_lookup 
on ac_lookup.activist_code_id = ac.activist_code_id 
left join
bernie_data_commons.master_xwalk mx 
on mx.myc_van_id = ac.myc_van_id 
and mx.state_code = ac.state_code
group by 1,2,3) activist_codes using(person_id)
where person_id is not null );
