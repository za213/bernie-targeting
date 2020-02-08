library(civis)
library(tidyverse)

# Google Sheet Import
sql_metadata <- "select universe_table, TO_DATE(pass_date, 'YYYY-MM-DD') as pass_date from gotv_universes.in_field_lists_metadata;"
metadata_df <- civis::read_civis(sql(sql_metadata), database = 'Bernie 2020') %>% 
  mutate(universe_table = gsub("\\'", "", tolower(universe_table)))  %>%
  mutate_at(vars('universe_table'),as.character)

# Find tables in schema
schema_list <- (paste0("('",paste(unique(gsub("\\..*","",metadata_df[[1]])), collapse = "','"),"')"))
sql_table_info <- paste0("select ordinal_position as position, column_name, t.table_schema , t.table_name 
from information_schema.tables t inner join information_schema.columns c
on c.table_name = t.table_name and c.table_schema = t.table_schema 
where (c.column_name = 'external_id' or  c.column_name = 'voterid') and t.table_schema = ",schema_list," order by ordinal_position")
table_info_df <- as.data.frame(civis::read_civis(sql(sql_table_info), database = 'Bernie 2020')) %>% 
  mutate(schema_table = tolower(paste0(table_schema,'.',table_name))) %>% 
  mutate_at(vars('schema_table','column_name'),as.character) %>%
  filter(schema_table %in% unique(metadata_df$universe_table)) %>%
  select(column_name, schema_table) %>%
  rename(pkey = column_name)
  
validation_tables <- inner_join(table_info_df, metadata_df, by = c('schema_table'='universe_table') ) %>%
  mutate(pass_date = as.character(pass_date))

queries <- c()
for (i in 1:nrow(validation_tables)) {
  id <- validation_tables[[1]][i]
  passdate <- validation_tables[[3]][i]
  tbl <- validation_tables[[2]][i]
  query <- paste0("(select regexp_replace(lower(",id,"), 'dnc_')::varchar as person_id, 
                           TO_DATE('",passdate,"', 'YYYY-MM-DD') as pass_date, '",tbl,"' as list_source from ",tbl,")")
  queries <- c(queries,query)
}

queries_unioned <- paste(queries, collapse = " UNION ALL ")

ccj_ids <- "(select person_id::varchar
            ,ccj_contactdate
            ,coalesce(ccj_contact_made,0) as ccj_contact_made
            ,coalesce(ccj_negative_result,0) as ccj_negative_result
            ,coalesce(ccj_id_1,0) as ccj_id_1
            ,coalesce(ccj_id_1_2,0) as ccj_id_1_2
            ,coalesce(ccj_id_2,0) as ccj_id_2
            ,coalesce(ccj_id_3,0) as ccj_id_3
            ,coalesce(ccj_id_4,0) as ccj_id_4
            ,coalesce(ccj_id_5,0) as ccj_id_5
            ,coalesce(ccj_id_1_2_3_4_5,0) as ccj_id_1_2_3_4_5
    FROM 
    (SELECT person_id::varchar 
            ,contactdate
            ,voter_state
            ,max(TO_DATE(contactdate, 'YYYY-MM-DD')) OVER (PARTITION BY person_id) AS ccj_contactdate
            ,CASE WHEN resultcode IN ('Canvassed', 'Do Not Contact', 'Refused', 'Call Back', 'Language Barrier', 'Hostile', 'Come Back', 'Cultivation', 'Refused Contact', 'Spanish', 'Other', 'Not Interested') THEN 1 ELSE 0 END AS ccj_contact_made 
            ,CASE WHEN resultcode IN ('Do Not Contact','Hostile','Refused','Refused Contact') OR (support_int = 4 OR support_int = 5) THEN 1 ELSE 0 END AS ccj_negative_result 
            ,CASE WHEN support_int = 1 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1 
            ,CASE WHEN support_int IN (1,2) AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1_2 
            ,CASE WHEN support_int = 2 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_2 
            ,CASE WHEN support_int = 3 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_3 
            ,CASE WHEN support_int = 4 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_4 
            ,CASE WHEN support_int = 5 AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_5 
            ,CASE WHEN support_int IN (1,2,3,4,5) AND unique_id_flag=TRUE THEN 1 ELSE 0 END AS ccj_id_1_2_3_4_5
            FROM bernie_data_commons.ccj_dnc 
            WHERE unique_id_flag = 1 AND person_id IS NOT NULL)  
    where person_id is not null and contactdate = ccj_contactdate)"

final_query <- paste0("DROP TABLE if exists gotv_universes.in_field_validation;
CREATE TABLE gotv_universes.in_field_validation distkey(person_id) sortkey(person_id) as 
(SELECT *
       ,NTILE(20) OVER (PARTITION BY state_code||list_source ORDER BY gotv_rank ASC) AS gotv_tiers_20
       from
     (select lists.person_id
             ,lists.list_source
             ,TO_DATE(lists.pass_date, 'YYYY-MM-DD') as pass_date
             ,ccj.ccj_contactdate
             ,coalesce(ccj.ccj_contact_made,0) as ccj_contact_made
             ,coalesce(ccj.ccj_negative_result,0) as ccj_negative_result
             ,coalesce(ccj.ccj_id_1,0) as ccj_id_1
             ,coalesce(ccj.ccj_id_2,0) as ccj_id_2
             ,coalesce(ccj.ccj_id_3,0) as ccj_id_3
             ,coalesce(ccj.ccj_id_4,0) as ccj_id_4
             ,coalesce(ccj.ccj_id_5,0) as ccj_id_5
             ,coalesce(ccj.ccj_id_1_2_3_4_5,0) as ccj_id_1_2_3_4_5
             ,case when ccj.ccj_contactdate is null then 'Uncontacted'
                   when datediff(d, TO_DATE(lists.pass_date, 'YYYY-MM-DD'), TO_DATE(ccj.ccj_contactdate, 'YYYY-MM-DD')) >= 0 
                   then 'Contacted after pass date' else 'Contacted before pass date' end as collected_after_list_pass
             ,base.state_code
             ,base.activist_flag
             ,base.activist_household_flag
             ,base.donor_1plus_flag
             ,base.donor_1plus_household_flag
             ,base.support_guardrail
             ,row_number() OVER (PARTITION BY base.state_code||lists.list_source  ORDER BY base.support_guardrail ASC, base.field_id_1_score DESC) as gotv_rank  
             from 
             (select * from (",queries_unioned,")) lists
             left join 
             (",ccj_ids,") ccj 
             using(person_id)
             left join
             bernie_data_commons.base_universe base
             using(person_id)));")

cat(final_query,file="sql.sql")

query_status <- query_civis(x=sql(final_query), database = "Bernie 2020")

aggregation_sql <- paste0("DROP TABLE if exists gotv_universes.in_field_validation_totals;
CREATE TABLE gotv_universes.in_field_validation_totals as 
(select 
state_code,
list_source,
pass_date,
collected_after_list_pass,
gotv_tiers_20,
coalesce(number_of_voters,0) as number_of_voters,
sum(coalesce(number_of_voters,0)) over (partition BY state_code||list_source||gotv_tiers_20) as number_of_voters_in_ventile,
coalesce(activists,0) as activists,
sum(coalesce(activists,0)) over (partition BY state_code||list_source||gotv_tiers_20) as activists_in_ventile,
round(coalesce(1.0*ccj_1/nullif(ccj_all,0),0),4) as ccj_1rate,
coalesce(ccj_1,0) as ccj_1,
coalesce(ccj_2,0) as ccj_2,
coalesce(ccj_3,0) as ccj_3,
coalesce(ccj_4,0) as ccj_4,
coalesce(ccj_5,0) as ccj_5,
coalesce(ccj_all,0) as ccj_all,
coalesce(ccj_contactmade,0) as ccj_contactmade,
coalesce(ccj_negativeresult,0) as ccj_negativeresult
from
(select
  state_code,
  list_source,
  pass_date,
  collected_after_list_pass,
  gotv_tiers_20,
  count(*) as number_of_voters,
  sum(case when activist_flag = 1
      OR activist_household_flag = 1
      OR donor_1plus_flag = 1 
      OR donor_1plus_household_flag = 1 then 1 end) as activists,
  sum(ccj_id_1) as ccj_1,
  sum(ccj_id_2) as ccj_2,
  sum(ccj_id_3) as ccj_3,
  sum(ccj_id_4) as ccj_4,
  sum(ccj_id_5) as ccj_5,
  sum(ccj_id_1_2_3_4_5) as ccj_all,
  sum(ccj_contact_made) as ccj_contactmade,
  sum(ccj_negative_result) as ccj_negativeresult
  from gotv_universes.in_field_validation  group by 1,2,3,4,5) order by 1,2,3,4,5)")

query_status <- query_civis(x=sql(aggregation_sql), database = "Bernie 2020")

