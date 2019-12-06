select

case when dup > 1 then 'duplicate'
else 'unique' end as dup
, count(*)
from
(select 
person_id,
row_number() over (partition by person_id ) as dup
 
  from phoenix_analytics.person p
    
    left join phoenix_analytics.person_votes pv using(person_id)
    left join phoenix_consumer.tsmart_consumer tc using(person_id)
    left join phoenix_scores.all_scores_2020 score using(person_id)

--    left join bernie_data_commons.apcd_dnc apcd using(person_id)
--    join bernie_data_commons.master_xwalk_dnc x on using(person_id)
--    left join l2.demographics l2 using(lalvoterid)


  where p.is_deceased = false
  and p.reg_record_merged = false
  and p.reg_on_current_file = true
  and p.reg_voter_flag = true
  ) group by 1 order by 2 desc
  
  
    

