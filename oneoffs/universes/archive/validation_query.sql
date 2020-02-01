select 
coalesce(gotv_univ_state,'Residual top 20% modeled support in '||state_code) as gotv_univ_list,
state_code,
round(1.0*first_choice_bernie/nullif(first_choice_any,0),3) as first_choice_bernie_share,
round(1.0*support_1_id/nullif(support_1_2_3_4_5_id,0),3) as support_1_id_share,
round(1.0*support_1_2_id/nullif(support_1_2_3_4_5_id,0),3) as support_1_2_id_share,
round(1.0*negative_result/nullif(contact_made,0),3) as negative_result_share,
round(1.0*ccj_id_1/nullif(ccj_id_1_2_3_4_5,0),3) as ccj_id_1_share,
round(1.0*ccj_id_1_2/nullif(ccj_id_1_2_3_4_5,0),3) as ccj_id_1_2_share,
first_choice_bernie,
first_choice_any,
support_1_id,
support_1_2_id,
support_1_2_3_4_5_id,
contact_made,
negative_result,
ccj_id_1,
ccj_id_1_2,
ccj_id_1_2_3_4_5
from 
(select 
gotv_univ_state,
state_code,
sum(first_choice_bernie) as first_choice_bernie,
sum(first_choice_any) as first_choice_any,

sum(support_1_id) as support_1_id,
sum(support_1_2_id) as support_1_2_id,
sum(support_1_2_3_4_5_id) as support_1_2_3_4_5_id,

sum(contact_made) as contact_made,
sum(negative_result) as negative_result,
sum(ccj_id_1) as ccj_id_1,
sum(ccj_id_1_2) as ccj_id_1_2,
sum(ccj_id_1_2_3_4_5) as ccj_id_1_2_3_4_5

from bernie_nmarchio2.gotv_validation_check
where current_support_raw_10 >= 8 or gotv_univ_state is not null group by 1,2
order by 2,1)
