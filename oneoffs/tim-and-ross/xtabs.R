
library(civis)
library(tidyverse)
library(ggplot2)

demo_query <- "create temp table list_xtab as (
 select 
  a.*,
  age_5way,
education_2way,
education_blockgroup_3way,
income_5way,
gender_2way,
urban_3way,
race_5way,
marital_2way,
party_3way,
ideology_5way,
primary_vote_2016_2way,
vote_history_5way,
early_voting_3way,
absentee_voting_4way,
fav_issue_area_3way,
fav_econ_issue_7way,
fav_poli_issue_5way,
fav_cultural_issue_10way
from
(select * from bernie_tryan.pro_bernie_1_id where support_1_id_100 >= 80) a left join 
bernie_data_commons.rainbow_analytics_frame r using(person_id)
);
      
select * from 
(

(select 'age_5way' as demo , age_5way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'education_2way' as demo , education_2way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'education_blockgroup_3way' as demo , education_blockgroup_3way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'income_5way' as demo , income_5way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'gender_2way' as demo , gender_2way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'urban_3way' as demo , urban_3way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'race_5way' as demo , race_5way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'marital_2way' as demo , marital_2way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'party_3way' as demo , party_3way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'ideology_5way' as demo , ideology_5way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'primary_vote_2016_2way' as demo , primary_vote_2016_2way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'vote_history_5way' as demo , vote_history_5way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'early_voting_3way' as demo , early_voting_3way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'absentee_voting_4way' as demo , absentee_voting_4way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'fav_issue_area_3way' as demo , fav_issue_area_3way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'fav_econ_issue_7way' as demo , fav_econ_issue_7way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'fav_poli_issue_5way' as demo , fav_poli_issue_5way as subgroup, count(*) from list_xtab group by 1,2)
union all
(select 'fav_cultural_issue_10way' as demo , fav_cultural_issue_10way as subgroup, count(*) from list_xtab group by 1,2)

) order by  demo, subgroup;"


df <- civis::read_civis(sql(demo_query), database = 'Bernie 2020') 

df_sub <- df %>% filter(subgroup != 'U') %>%
  tidyr::separate(subgroup, c("order","subgroup"), sep = " - ", extra = "merge") %>%
  group_by(demo) %>%
  mutate(total = sum(count))%>%
  ungroup() %>%
  mutate(share = round(count/total, digits = 2)*100) %>% #paste0(sprintf("%.00f", round(count/total, digits = 2)*100),"%")) %>%
  filter(order != '') %>%
  select( order, share, subgroup)

