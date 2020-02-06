

querysql <- "select
state_code, 
gotv_segment_validation,
gotv_segment_juiced,
pturnout_2016 as primary_turnout_2016,
ccj_holdout_id,
activist_flag,
vote_ready_6way,
donor_id,
count(*) as number_of_voters,
sum(activist_flag) as activists,
sum(ccj_id_1) as ccj1,
sum(ccj_id_2) as ccj2,
sum(ccj_id_3) as ccj3,
sum(ccj_id_4) as ccj4,
sum(ccj_id_5) as ccj5,
sum(ccj_id_1_2_3_4_5) as ccj1all,
sum(ccj_id_1_2) as ccj12,
avg(field_id_1_score_100) as field1score,
avg(current_support_raw_100) as supportscore
from
bernie_nmarchio2.base_universe group by 1,2,3,4,5,6,7,8"

df_qc <- civis::read_civis(sql(querysql ), database = 'Bernie 2020') 

names(df_qc)

df_qc_out <- df_qc %>%
  #filter(ccj_holdout_id == 1) %>%
  #filter(vote_ready_6way %in% c('1 - Vote-ready','2 - Vote-ready lapsed')) %>%
  group_by(state_code, ccj_holdout_id, gotv_segment_validation) %>% # 
  #group_by(state_code, gotv_segment_juiced) %>% 
  dplyr::summarize_at(vars(number_of_voters,ccj1,ccj2,ccj3,ccj4,ccj5,ccj1all,ccj12), funs(sum), na.rm=TRUE)  %>%
  ungroup() %>%
  mutate(ccj1rate = ccj1/ccj1all)


write_csv(df_qc_out, '/Users/nm/Desktop/rates.csv')


criteria_search <- "select
state_code,
current_support_raw_100,
field_id_1_score_100,
field_id_composite_score_100,
turnout_current_100,
sanders_strong_support_score_100,
sanders_very_excited_score_100,
biden_support_100,
warren_support_100,
buttigieg_support_100,
spoke_support_1box_100,
spoke_persuasion_1plus_100,
spoke_persuasion_1minus_100,
attendee_100,
kickoff_party_rally_barnstorm_attendee_100,
canvasser_phonebank_attendee_100,
bernie_action_100,
1 as number_of_voters,
donor_id,
activist_flag,
ccj_id_1,
ccj_id_1_2,
field_id_1_score_100,
current_support_raw_100,
civis_2018_one_pct_persuasion,
civis_2018_marijuana_persuasion,
civis_2018_college_persuasion,
civis_2018_welcome_persuasion,
civis_2018_sexual_assault_persuasion,
civis_2018_economic_persuasion
from bernie_nmarchio2.base_universe where ccj_holdout_id = 1 order by random() limit 200000"
  
df_search <- civis::read_civis(sql(criteria_search ), database = 'Bernie 2020') 


library(pROC)
library(civis)
library(dplyr)
library(reshape2)

names(df_search)
df_search2 <- df_search %>% filter(state_code != 'CA')
state_list <- unique(df_search2$state_code)

# Plots
df_search_long <-df_search2 %>% select(current_support_raw_100,
                                        field_id_1_score_100,
                                        field_id_composite_score_100,
                                        turnout_current_100,
                                        sanders_strong_support_score_100,
                                        sanders_very_excited_score_100,
                                        biden_support_100,
                                        warren_support_100,
                                        buttigieg_support_100,
                                        spoke_support_1box_100,
                                        spoke_persuasion_1plus_100,
                                        spoke_persuasion_1minus_100,
                                        attendee_100,
                                        kickoff_party_rally_barnstorm_attendee_100,
                                        canvasser_phonebank_attendee_100,
                                        bernie_action_100,
                                        ccj_id_1) %>% melt(id.vars = 'ccj_id_1')

(ggplot2::ggplot(df_search_long, ggplot2::aes(value, color = as.character(ccj_id_1) )) + 
    ggplot2::geom_density( alpha = .6) +
    ggplot2::scale_x_continuous( ) +
    ggplot2::theme_bw() +
    ggplot2::labs(y= "Density", 
                  x = 'Percentiles', 
                  caption = paste(paste0(state_list,collapse=","),' ',nrow(df_search), 'n-size')) +
    scale_color_discrete(name = "1 = 1 IDs") +
    ggplot2::theme(legend.position ="bottom",
                   text = ggplot2::element_text(size = 13, color = "#161616"))+
    facet_wrap(vars(variable)))


names(df_search)
# biden < 80
# support raw > 90 
# field 1> 80 
# field 1 composite > 80
# sanders strong > 80 
# spoke support 1 box > 75
# spoke persuasion box  > 75
# spoke 1 minus < 50 
# attendee_100 > 80 (any ak model)


