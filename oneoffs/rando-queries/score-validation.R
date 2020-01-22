
q3 <- "select *,
NTILE(100) OVER (ORDER BY support_score) AS support_score_100,
NTILE(100) OVER (ORDER BY current_support_raw) AS current_support_raw_100,
NTILE(100) OVER (ORDER BY current_support_composite) AS current_support_composite_100,
NTILE(100) OVER (ORDER BY field_id_1_score) AS field_id_1_score_100
from
(select * from bernie_nmarchio2.test where voterbase_id is not null) a
left join
(select voterbase_id,
  support_score
  from bernie_nmarchio2.sample_appended) b
using(voterbase_id)
left join 
(select person_id,current_support_raw,
current_support_composite,field_id_1_score
   from bernie_data_commons.all_scores)
using(person_id)
left join 
(select distinct person_id, engagement_segment from bernie_nmarchio2.engagement_analytics where person_id  is not null)
using(person_id)
left join
(select person_id, canvasser_phonebank_attendee_100 from bernie_nmarchio2.actionpop_output_20191220)
using(person_id)
where support_score is not null and current_support_raw is not null"

library(pROC)
library(civis)
library(dplyr)
library(reshape2)

df <- civis::read_civis(sql(q3), database = 'Bernie 2020') 

#state_list <- c('AL','AR','NC','OK','TN','VA','TX','CO','ME','MA','MN','VT','UT')
state_list <- c('NH','NV','SC')
#state_list <- c('CA')
  
df_test <- df %>% 
  mutate(contactdate_field=as.Date(contactdate_field,format="%Y-%m-%d"),
         survey_date=as.Date(survey_date,format="%Y-%m-%d")) %>%
  filter(state_coalesced %in% state_list) %>%
  mutate(support_int_1_0 = case_when(support_int == 1 ~ 1,
                                     support_int == 2 ~ 1,
                                     TRUE ~ 0),
         support_int_field_1_0 = case_when(support_int_field == 1 ~ 1,
                                     support_int_field == 2 ~ 1,
                                     TRUE ~ 0),
         in_ak_mob_myc_bern_1_0 = case_when(engagement_segment == '8 - Supporter from Spoke' ~ 0,
                                            engagement_segment == '' ~ 0,
                                           TRUE ~ 1),
         vol_target_1_0 = case_when(canvasser_phonebank_attendee_100 >= 80 ~ 1,
                                            TRUE ~ 0))


df_field <- df_test %>% filter(support_int_field > 0)
df_3rd_party <- df_test %>% filter(support_int > 0)
df_vols <- df_test
#  'IA',
# 
# round(cor(df_vols %>% select(support_score,current_support_raw,vol_target_1_0,in_ak_mob_myc_bern_1_0 )),2)
# round(cor(df_3rd_party %>% select( support_int_1_0,support_score,current_support_raw)),2)
# round(cor(df_field %>% select(support_int_field_1_0,support_score,current_support_raw)),2)

#Civis
(auc_3rd_civis <- round(auc(roc(df_3rd_party$support_int_1_0, df_3rd_party$support_score)),3))
(auc_field_civis <- round(auc(roc(df_field$support_int_field_1_0, df_field$support_score) ),3))
(auc_crm_civis <- round(auc(roc(df_vols$in_ak_mob_myc_bern_1_0,df_vols$support_score) ),3))
(auc_voltarget_civis <- round(auc(roc(df_vols$vol_target_1_0,df_vols$support_score)),3))
#Benchmark
(auc_3rd_bench <- round(auc(roc(df_3rd_party$support_int_1_0, df_3rd_party$current_support_raw/100) ),3))
(auc_field_bench <- round(auc(roc(df_field$support_int_field_1_0, df_field$current_support_raw/100) ),3))
(auc_crm_bench <- round(auc(roc(df_vols$in_ak_mob_myc_bern_1_0,df_vols$current_support_raw/100) ),3))
(auc_voltarget_bench <- round(auc(roc(df_vols$vol_target_1_0,df_vols$current_support_raw/100) ),3))

# Plots
df_3rd_party_long <- df_3rd_party %>% select(current_support_raw_100, support_score_100, support_int_1_0) %>% melt(id.vars = 'support_int_1_0')

ggplot2::ggplot(df_3rd_party_long, ggplot2::aes(value, color = as.character(support_int_1_0) )) + 
  ggplot2::geom_density( alpha = .6) +
  ggplot2::scale_x_continuous( ) +
  ggplot2::theme_bw() +
  ggplot2::labs(y= "Density", 
                x = 'Percentiles', 
                subtitle = paste('Third party IDs','\nAUC: current_support_raw (',auc_3rd_bench,')  support_score (',auc_3rd_civis,')'),
                caption = paste(paste0(state_list,collapse=","),' ',nrow(df_3rd_party), 'n-size')) +
  scale_color_discrete(name = "1 = (1,2 IDs)") +
  ggplot2::theme(legend.position ="bottom",
                 text = ggplot2::element_text(size = 13, color = "#161616"))+
  facet_wrap(vars(variable))

df_field_long <- df_field %>% select(current_support_raw_100, support_score_100, support_int_field_1_0) %>% melt(id.vars = 'support_int_field_1_0')

ggplot2::ggplot(df_field_long , ggplot2::aes(value, color = as.character(support_int_field_1_0) )) + 
  ggplot2::geom_density( alpha = .6) +
  ggplot2::scale_x_continuous( ) +
  ggplot2::theme_bw() +
  ggplot2::labs(y= "Density", 
                x = 'Percentile', 
                subtitle = paste('Field IDs','\nAUC: current_support_raw (',auc_field_bench,')  support_score (',auc_field_civis,')'),
                caption = paste(paste0(state_list,collapse=","),' ',nrow(df_field), 'n-size')) +
  scale_color_discrete(name = "1 = (1,2 IDs)") +
  ggplot2::theme(legend.position ="bottom",
                 text = ggplot2::element_text(size = 13, color = "#161616"))+
  facet_wrap(vars(variable))

df_vols_long <- df_vols %>% select(current_support_raw_100, support_score_100, in_ak_mob_myc_bern_1_0, vol_target_1_0) %>% melt(id.vars = c('in_ak_mob_myc_bern_1_0','vol_target_1_0'))

ggplot2::ggplot(df_vols_long , ggplot2::aes(value, color = as.character(in_ak_mob_myc_bern_1_0) )) + 
  ggplot2::geom_density( alpha = .6) +
  ggplot2::scale_x_continuous( ) +
  ggplot2::theme_bw() +
  ggplot2::labs(y= "Density", 
                x = 'Percentile', 
                subtitle = paste('AK/Mob/Bern/MyC Vol IDs','\nAUC: current_support_raw (',auc_crm_bench,')  support_score (',auc_crm_civis,')'),
                caption = paste(paste0(state_list,collapse=","),' ',nrow(df_vols), 'n-size')) +
  scale_color_discrete(name = "1 = True") +
  ggplot2::theme(legend.position ="bottom",
                 text = ggplot2::element_text(size = 13, color = "#161616"))+
  facet_wrap(vars(variable))

ggplot2::ggplot(df_vols_long , ggplot2::aes(value, color = as.character(vol_target_1_0) )) + 
  ggplot2::geom_density( alpha = .6) +
  ggplot2::scale_x_continuous( ) +
  ggplot2::theme_bw() +
  ggplot2::labs(y= "Density", 
                x = 'Percentile', 
                subtitle = paste('Top 20% Nico Vol Scores','\nAUC: current_support_raw (',auc_voltarget_bench,')  support_score (',auc_voltarget_civis,')'),
                caption = paste(paste0(state_list,collapse=","),' ',nrow(df_vols), 'n-size')) +
  scale_color_discrete(name = "1 = True") +
  ggplot2::theme(legend.position ="bottom",
                 text = ggplot2::element_text(size = 13, color = "#161616"))+
  facet_wrap(vars(variable))
