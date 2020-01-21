
q3 <- "select 
case when support_int_field = 1 then 1 else 0 end as support_ind_field_1_0,
NTILE(100) OVER (ORDER BY support_score) AS support_score_100,
NTILE(100) OVER (ORDER BY current_support_raw) AS current_support_raw_100,
NTILE(100) OVER (ORDER BY current_support_composite) AS current_support_composite_100,
NTILE(100) OVER (ORDER BY field_id_1_score) AS field_id_1_score_100,
        *  from
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
where support_score is not null and current_support_raw is not null "

library(pROC)
library(civis)
library(dplyr)

df3 <- civis::read_civis(sql(q3), database = 'Bernie 2020') 

df_test <- df3 %>% 
  mutate(contactdate_field=as.Date(contactdate_field,format="%Y-%m-%d")) %>%
  filter(support_int_field > 0,
         state_coalesced %in% c('SC','AL','AR','CA','CO','ME','MA','MN','NC','OK','TN','TX','UT','VT','VA'),
         contactdate_field > '2019-12-23' )
#'IA','NH','NV',

#Civis
roc_obj <- roc(df_test$support_ind_field_1_0, 
               df_test$support_score)
auc(roc_obj)

#Haystaq 
roc_obj <- roc(df_test$support_ind_field_1_0, 
               df_test$current_support_raw/100)
auc(roc_obj)

roc_obj <- roc(df_test$support_ind_field_1_0, 
               df_test$field_id_1_score/100)
auc(roc_obj)

# Plots
ggplot2::ggplot(df_test, ggplot2::aes(current_support_raw_100, color = as.character(support_int_field) )) + 
  ggplot2::geom_density( alpha = .6) +
  ggplot2::scale_x_continuous( ) +
  ggplot2::theme_bw() +
  ggplot2::theme(legend.position ="bottom",
                 legend.title = ggplot2::element_blank(),
                 text = ggplot2::element_text(size = 13, color = "#161616"))

ggplot2::ggplot(df_test, ggplot2::aes(field_id_1_score_100, color = as.character(support_int_field) )) + 
  ggplot2::geom_density( alpha = .6) +
  ggplot2::scale_x_continuous( ) +
  ggplot2::theme_bw() +
  ggplot2::theme(legend.position ="bottom",
                 legend.title = ggplot2::element_blank(),
                 text = ggplot2::element_text(size = 13, color = "#161616"))

ggplot2::ggplot(df_test, ggplot2::aes(support_score_100, color = as.character(support_int_field) )) + 
  ggplot2::geom_density( alpha = .6) +
  ggplot2::scale_x_continuous( ) +
  ggplot2::theme_bw() +
  ggplot2::theme(legend.position ="bottom",
                 legend.title = ggplot2::element_blank(),
                 text = ggplot2::element_text(size = 13, color = "#161616"))
