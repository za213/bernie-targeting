
drop table if exists bernie_nmarchio2.message_aggregates;

create table bernie_nmarchio2.message_aggregates distkey(campaign_contact_id) sortkey(campaign_contact_id) as
(select *
,case when text_stripped similar to ('%legalize marijuana%') then 1 else 0 end as marijuana_message
,case when text_stripped similar to ('%climate change%') then 1 else 0  end as climate_message
,case when text_stripped similar to ('%Green New Deal%') then 1 else 0  end as green_new_deal_message
,case when text_stripped similar to ('%Big Pharma has been ripping off%') then 1 else 0  end as big_pharma_message
,case when text_stripped similar to ('health care') then 1 else 0  end as health_care_message
,case when text_stripped similar to ('car insurance is unaffordable') then 1 else 0  end as car_insurance_message
,case when text_stripped similar to ('%Young people are leading the movement%|%building a movement%|%grassroots energy%|%movement of volunteers, donors, and supporters%') then 1 else 0 end as 
,case when text_stripped similar to ('%HUGE NEWS: Bernie JUST announced%|%running for PRESIDENT%|%Bernie JUST announced%') then 1 else 0 end as annoucement_message
,case when text_stripped similar to ('%Iowa, New Hampshire, Nevada,%') then 1 else 0 end as first_3_wins_message
,case when text_stripped similar to ('%Trump%') then 1 else 0 end as beat_trump_message
,case when text_stripped similar to ('%siempre ha luchado por la clase trabajadora%|%esforzado toda su vida en luchar por los derechos de los trabajadores%') then 1 else 0 end as fight_for_workers_spanish_message
,case when text_stripped similar to ('%fighting against greed and corruption%|%against greed and corruption for decades%') then 1 else 0 end as fight_against_greed_corrupt_message
,case when text_stripped similar to ('%corrupt political and economic system that needs major change%') then 1 else 0 end as corrupt_system_needs_major_change_message
,case when text_stripped similar to ('%on the side of working people against a corrupt corporate elite%') then 1 else 0 end as working_people_against_corpelite_message
,case when text_stripped similar to ('%standing up to the greed and corruption of the corporate elite%' then 1 else 0 end as standing_up_to_corpelite
,case when text_stripped similar to ('%take on the billionaire class%') then 1 else 0 end as take_on_billionaire_class_message
,case when text_stripped similar to ('%work for all of us, not just wealthy campaign contributors%') then 1 else 0 end as work_for_all_not_wealthy_message
,case when text_stripped similar to ('%take on the billionaire class%|%corporate elite%|%wealthy campaign contributors%|%corrupt political%|%greed and corruption%') then 1 else 0 end as strong_class_based_language_message 
 from
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



select * from
(select text_stripped, count(*), sum(case when support_int = 1 then 1 end) as id_1,
sum(case when support_int in (1,2,3,4,5) then 1 end) as id_all, 1.0*id_1/id_all as rate1
from bernie_nmarchio2.message_aggregates where support_int in (1,2,3,4,5) group by 1) where id_all > 2000  order by rate1 desc


