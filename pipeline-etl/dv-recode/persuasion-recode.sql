
drop table if exists bernie_nmarchio2.message_aggregates;

create table bernie_nmarchio2.message_aggregates distkey(campaign_contact_id) sortkey(campaign_contact_id) as
(select * from
(select *, 
regexp_replace(text, '((Hi|Hey|Hola|This is).*(!|.|,) ((This is)|(Just wanted)|(quería)|(there\'s)|(It\'s)|(it\'s)|(soy)) ([^.!my]+))') as text_stripped 
  from 
(select 
  campaign_contact_id,
  text,
  row_number() over (partition by campaign_contact_id order by sent_at asc) as message_order, 
  count(*) over (partition by campaign_contact_id) as convo_length,
  is_from_contact
 from spoke.message where send_status = 'DELIVERED') where message_order = 1 and convo_length > 1 and is_from_contact = 'f')
left join
 (select person_id,externalcontactid,resultcode,support_id,support_int,support_response_text from bernie_data_commons.ccj_dnc where contacttype  = 'spoke' )
 on externalcontactid = campaign_contact_id where support_response_text is not null);



