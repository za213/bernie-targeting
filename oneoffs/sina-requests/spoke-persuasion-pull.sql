(select 
      rank.spoke_persuasion_1plus_100
      ,rank.person_id 	as person_id
      ,phx.first_name			as voters_firstname
      ,phx.last_name			as voters_lastname
      ,apcd.phone			as votertelephones_fullphone
      ,phx.voting_city 		as residence_addresses_city
      ,phx.voting_zip 		as residence_addresses_zip
      ,phx.age_combined 		as voters_age
      ,phx.gender_combined 		as voters_gender
      ,phx.party_name_dnc 		as parties_description
      ,phx.state_code			as state_code 
      ,rank.spoke_support_1box
      ,rank.spoke_persuasion_1plus
      ,rank.spoke_persuasion_1minus
      ,rank.spoke_support_1box_100
      
      ,rank.spoke_persuasion_1minus_100
      
      from (select * from bernie_nmarchio2.spoke_output_20191221
            where spoke_support_1box_100 >= 50 and 
            spoke_persuasion_1plus_100 >= 80 and 
            spoke_persuasion_1minus_100 <= 50  
            order by spoke_persuasion_1plus desc) rank
      
      join phoenix_analytics.person phx
      on phx.person_id = rank.person_id
      join (
        select person_id
        ,phone
        from bernie_data_commons.apcd_dnc
        where phone_rank_for_person = 1
      ) apcd on apcd.person_id = rank.person_id
      
      --dialer_passes: https://platform.civisanalytics.com/spa/#/scripts/sql/45804908 
        --dialer_passes_stats: https://platform.civisanalytics.com/spa/#/scripts/sql/48038247
        
        join bernie_data_commons.dialer_passes_stats pass 
      on pass.phone = apcd.phone
      
      where pass.attempts_last_7_days < 1
      and pass.total_contacts < 8
      and pass.negative_result_total = 0
      and rank.person_id not in (
        
        select person_id
        from bernie_data_commons.contactcontacts_joined ccj
        where person_id is not null
        and (resultcode in ('Do Not Contact', 'Hostile', 'Refused','Refused contact', 'Out of Order', 'Wrong Number')
             or support_id is not null)
        group by 1 
      )
      
      and apcd.phone not in (		
        
        select phone
        from contacts.contactscontact cc
        where phone is not null
        and resultcode in ('Do Not Contact', 'Hostile', 'Refused','Refused contact', 'Out of Order', 'Wrong Number')
        group by 1 
      )
      
      and phx.state_code in ('SC')
      order by 1 desc) 
