

select person_id, 
gotv_rank as gotv_rank_order
'list name' as list_name, 
'list cutter' as user_name, 
TO_DATE('2020-02-05', 'YYYY-MM-DD') as date_in_field
from gotv_universes.ut_listname;
