
library(civis)
library(tidyverse)
library(ggplot2)

demo_query <- "create temp table list_xtab as (
 select 
person_id,
state_code,
age_5way,
education_2way,
income_5way,
gender_2way,
urban_3way,
race_5way,
marital_2way,
party_3way,
vote_history_6way,
early_vote_history_3way,
registered_in_state_3way,
dem_primary_eligible_2way,
vote_ready_5way,
gotv_tiers_20
from
(select * from bernie_nmarchio2.base_universe_qc));


select * from 
(
(select state_code, gotv_tiers_20,'age_5way' as demo , age_5way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'education_2way' as demo , education_2way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'income_5way' as demo , income_5way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'gender_2way' as demo , gender_2way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'urban_3way' as demo , urban_3way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'race_5way' as demo , race_5way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'marital_2way' as demo , marital_2way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'party_3way' as demo , party_3way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'vote_history_6way' as demo , vote_history_6way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'early_vote_history_3way' as demo , early_vote_history_3way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'registered_in_state_3way' as demo , registered_in_state_3way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'dem_primary_eligible_2way' as demo , dem_primary_eligible_2way as subgroup, count(*) from list_xtab group by 1,2,3,4)
union all
(select state_code, gotv_tiers_20,'vote_ready_5way' as demo , vote_ready_6way as subgroup, count(*) from list_xtab group by 1,2,3,4)
) order by  demo, subgroup;"

df <- civis::read_civis(sql(demo_query), database = 'Bernie 2020') 

df_sub <- df %>% filter(subgroup != 'U') %>%
  tidyr::separate(subgroup, c("order","subgroup"), sep = " - ", extra = "merge") %>%
  filter(subgroup != 'Unknown') %>%
  group_by(demo, gotv_tiers_20, state_code) %>%
  mutate(total = sum(count))%>%
  ungroup() %>%
  group_by(demo, subgroup, state_code) %>%
  mutate(total_subgroup = sum(count))%>%
  ungroup() %>%
  group_by(state_code, demo) %>%
  mutate(total_demo = sum(count))%>%
  ungroup() %>%
  mutate(share_state = round(count/total, digits = 2),
         share_total = round(total_subgroup/total_demo, digits = 2),
         index_to_state = share_state/share_total) %>% 
  filter(order != '')

write_csv(df_sub, '/Users/nm/Desktop/table.csv')
