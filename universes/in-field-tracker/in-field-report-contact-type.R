library(civis)
library(tidyverse)

# Google Sheet Import
sql_metadata <- "select universe_table, TO_DATE(pass_date, 'YYYY-MM-DD') as pass_date, state state_code from gotv_universes.in_field_lists_metadata;"
metadata_df <- civis::read_civis(sql(sql_metadata), database = 'Bernie 2020') %>% 
  mutate(universe_table = gsub("\\'", "", tolower(universe_table)))  %>%
  mutate_at(vars('universe_table'),as.character)

# Find tables in schema
schema_list <- (paste0("('",paste(unique(gsub("\\..*","",metadata_df[[1]])), collapse = "','"),"')"))
sql_table_info <- paste0("select ordinal_position as position, column_name, t.table_schema , t.table_name 
from information_schema.tables t inner join information_schema.columns c
on c.table_name = t.table_name and c.table_schema = t.table_schema 
where (c.column_name = 'external_id' or  c.column_name = 'voterid' or c.column_name = 'person_id') and t.table_schema = ",schema_list," order by ordinal_position")
table_info_df <- as.data.frame(civis::read_civis(sql(sql_table_info), database = 'Bernie 2020')) %>% 
  mutate(schema_table = tolower(paste0(table_schema,'.',table_name))) %>% 
  mutate_at(vars('schema_table','column_name'),as.character) %>%
  filter(schema_table %in% unique(metadata_df$universe_table)) %>%
  select(column_name, schema_table) %>%
  rename(pkey = column_name)
  
validation_tables <- inner_join(table_info_df, metadata_df, by = c('schema_table'='universe_table') ) %>%
  mutate(pass_date = as.character(pass_date))

print(validation_tables)

queries <- c()
for (i in 1:nrow(validation_tables)) {
  id <- validation_tables[[1]][i]
  passdate <- validation_tables[[3]][i]
  tbl <- validation_tables[[2]][i]
  state_code <- validation_tables[[4]][i]
  query <- paste0("(select regexp_replace(lower(",id,"), 'dnc_')::varchar as person_id, 
                           TO_DATE('",passdate,"', 'YYYY-MM-DD') as pass_date, '",state_code,"' as state_code, '",tbl,"' as list_source from ",tbl,")")
  queries <- c(queries,query)
}

queries_unioned <- paste(queries, collapse = " UNION ALL ")

ccj_ids <- "(select person_id::varchar person_id
     ,contactcontact_id
     ,most_recent_canvass
     ,most_recent_attempt
     ,contactdate
     ,contacttype
     ,coalesce(ccj_contact_made,0) as ccj_contact_made
     ,coalesce(ccj_negative_result,0) as ccj_negative_result
     ,coalesce(ccj_id_1,0) as ccj_id_1
     ,coalesce(ccj_id_1_2,0) as ccj_id_1_2
     ,coalesce(ccj_id_2,0) as ccj_id_2
     ,coalesce(ccj_id_3,0) as ccj_id_3
     ,coalesce(ccj_id_4,0) as ccj_id_4
     ,coalesce(ccj_id_5,0) as ccj_id_5
     ,coalesce(ccj_id_1_2_3_4_5,0) as ccj_id_1_2_3_4_5
     ,case when contactdate = most_recent_canvass then 1 else 0 end most_recent
     ,max(ccj_contact_made) over (partition by person_id, contacttype) ever_contacted
FROM (
    SELECT person_id::varchar
         ,contactcontact_id
         ,contactdate
         ,voter_state
         ,case when contacttype in ('pdi_mobile', 'bern_app_crowd_canvass','minivan_doors', 'minivan_paid_doors','myc-Paid Walk', 'myc_minivan_doors', 'myv-Paid Walk') then 'canvasses'
             else contacttype end contacttype
         ,CASE WHEN resultcode IN ('Canvassed', 'Do Not Contact', 'Refused', 'Call Back', 'Language Barrier', 'Hostile', 'Come Back', 'Cultivation', 'Refused Contact', 'Spanish', 'Other', 'Not Interested') THEN 1 ELSE 0 END AS ccj_contact_made
         ,max(TO_DATE(contactdate, 'YYYY-MM-DD')) OVER (PARTITION BY person_id, contacttype) AS most_recent_attempt
         ,max(case when ccj_contact_made = 1 then TO_DATE(contactdate, 'YYYY-MM-DD') else null end) OVER (PARTITION BY person_id, contacttype) AS most_recent_canvass
         ,CASE WHEN resultcode IN ('Do Not Contact','Hostile','Refused','Refused Contact') OR ((support_int = 4 OR support_int = 5) AND mrc = 1) THEN 1 ELSE 0 END AS ccj_negative_result
         ,CASE WHEN support_int = 1 AND mrc = 1 THEN 1 ELSE 0 END AS ccj_id_1
         ,CASE WHEN support_int IN (1,2) AND mrc = 1 THEN 1 ELSE 0 END AS ccj_id_1_2
         ,CASE WHEN support_int = 2 AND mrc = 1 THEN 1 ELSE 0 END AS ccj_id_2
         ,CASE WHEN support_int = 3 AND mrc = 1 THEN 1 ELSE 0 END AS ccj_id_3
         ,CASE WHEN support_int = 4 AND mrc = 1 THEN 1 ELSE 0 END AS ccj_id_4
         ,CASE WHEN support_int = 5 AND mrc = 1 THEN 1 ELSE 0 END AS ccj_id_5
         ,CASE WHEN support_int IN (1,2,3,4,5) AND mrc = 1 THEN 1 ELSE 0 END AS ccj_id_1_2_3_4_5
    FROM (
        select *,
               row_number() over (partition by person_id, contacttype, case when support_int is not null then 1 else 0 end order by contacttimestamp desc) mrc
        from bernie_data_commons.ccj_dnc
        ) mr
    WHERE person_id IS NOT NULL
      and contacttype in ('pdi_mobile', 'bern_app_crowd_canvass','minivan_doors', 'minivan_paid_doors','myc-Paid Walk', 'myc_minivan_doors', 'myv-Paid Walk', 'getthru_dialer', 'spoke')
    ) x
where person_id is not null)"

final_query <- paste0("DROP TABLE if exists gotv_universes.in_field_validation_breakdown;
CREATE TABLE gotv_universes.in_field_validation_breakdown distkey(person_id) sortkey(person_id) as 
(SELECT *
from (
    select lists.person_id
         ,lists.list_source
         ,TO_DATE(lists.pass_date, 'YYYY-MM-DD') as pass_date
         ,ccj.contactdate
         ,lists.contacttype
         ,contactcontact_id
         ,coalesce(ccj.ccj_contact_made,0) as ccj_contact_made
         ,case when most_recent_attempt >= pass_date then 1 else 0 end attempted
         ,case when most_recent_canvass >= pass_date then 1 else 0 end canvassed
         ,coalesce(ccj.ccj_negative_result,0) as ccj_negative_result
         ,coalesce(ccj.ccj_id_1,0) as ccj_id_1
         ,coalesce(ccj.ccj_id_2,0) as ccj_id_2
         ,coalesce(ccj.ccj_id_3,0) as ccj_id_3
         ,coalesce(ccj.ccj_id_4,0) as ccj_id_4
         ,coalesce(ccj.ccj_id_5,0) as ccj_id_5
         ,coalesce(ccj.ccj_id_1_2_3_4_5,0) as ccj_id_1_2_3_4_5

         ,case when ccj.contactcontact_id is null then '4 - Unattempted'
             when ccj.ever_contacted = 0 then '3 - Uncanvassed'
             when datediff(d, TO_DATE(lists.pass_date, 'YYYY-MM-DD'), TO_DATE(ccj.most_recent_canvass, 'YYYY-MM-DD')) >= 0 then '1 - Contacted after pass date'
             else '2 - Contacted before pass date' end as collected_after_list_pass
         ,coalesce(lists.state_code, base.state_code) state_code
         ,base.activist_flag
         ,base.activist_household_flag
         ,base.donor_1plus_flag
         ,base.donor_1plus_household_flag
         ,base.support_guardrail
    from (
        select *
        from (
            select *
            from (",queries_unioned,")
            ) l
        cross join (
            select 'canvasses' contacttype
            union
            select 'getthru_dialer'
            union
            select 'spoke'
            ) ct
        ) lists
        left join  (",ccj_ids,") ccj on lists.person_id = ccj.person_id
                                   and lists.contacttype = ccj.contacttype
        left join bernie_data_commons.base_universe base on lists.person_id = base.person_id
    ) x);")

cat(final_query,file="sql.sql")

query_status <- query_civis(x=sql(final_query), database = "Bernie 2020")

aggregation_sql <- paste0("DROP TABLE if exists gotv_universes.in_field_validation_breakdown_totals;
CREATE TABLE gotv_universes.in_field_validation_breakdown_totals as 
(select x.state_code,
       x.list_source,
       x.pass_date,
       x.collected_after_list_pass,
       contacttype,
       coalesce(number_of_voters_in_ventile,0) as number_of_voters_in_ventile,
       coalesce(number_of_voters,0) as number_of_voters,

       coalesce(number_of_voters_attempted, 0) as unique_attempts,
       coalesce(all_attempts_in_pass_period, 0) as all_attempts,
       round(coalesce(1.0*unique_attempts/nullif(number_of_voters,0),0),4) as percent_attempted,
       round(coalesce(1.0*unique_attempts/nullif(all_attempts,0),0),4) as percent_unique_attempts,

       coalesce(activists_in_ventile,0) as activists_in_ventile,
       coalesce(activists,0) as activists,

       round(coalesce(1.0*ccj_1/nullif(ccj_all,0),0),4) as ccj_1rate,
       coalesce(ccj_1,0) as ccj_1,
       coalesce(ccj_2,0) as ccj_2,
       coalesce(ccj_3,0) as ccj_3,
       coalesce(ccj_4,0) as ccj_4,
       coalesce(ccj_5,0) as ccj_5,
       coalesce(ccj_all,0) as ccj_all,

       coalesce(ccj_negativeresult,0) as ccj_negativeresult,
       coalesce(total_contacts,0) as all_time_contacts
from (
    select state_code,
           list_source,
           pass_date,
           collected_after_list_pass,
           contacttype,
           count(distinct case when attempted = 1 then person_id end) number_of_voters_attempted,
           count(distinct person_id) as number_of_voters,

           count(distinct case when contactdate >= pass_date then contactcontact_id end) all_attempts_in_pass_period,

           count(distinct case when activist_flag = 1
               OR activist_household_flag = 1
               OR donor_1plus_flag = 1
               OR donor_1plus_household_flag = 1 then person_id end) as activists,
           sum(ccj_id_1) as ccj_1,
           sum(ccj_id_2) as ccj_2,
           sum(ccj_id_3) as ccj_3,
           sum(ccj_id_4) as ccj_4,
           sum(ccj_id_5) as ccj_5,
           sum(ccj_id_1_2_3_4_5) as ccj_all,

           sum(ccj_contact_made) as total_contacts,
           sum(ccj_negative_result) as ccj_negativeresult
    from gotv_universes.in_field_validation_breakdown
    group by 1,2,3,4,5
    ) x
    left join (
        select state_code,
               list_source,
               pass_date,
               count(distinct person_id) number_of_voters_in_ventile,
               count(distinct case when activist_flag = 1
                                              OR activist_household_flag = 1
                                              OR donor_1plus_flag = 1
                                              OR donor_1plus_household_flag = 1 then person_id end)  activists_in_ventile


        from gotv_universes.in_field_validation_breakdown
        group by 1,2,3
        ) t on x.state_code = t.state_code
                   and x.list_source = t.list_source
                   and x.pass_date = t.pass_date
order by 1,2,3)")

query_status <- query_civis(x=sql(aggregation_sql), database = "Bernie 2020")



state_aggregation_sql <- paste0("DROP TABLE if exists gotv_universes.in_field_validation_breakdown_totals_state;
CREATE TABLE gotv_universes.in_field_validation_breakdown_totals_state as 
(select x.state_code,
       x.collected_after_list_pass,
       contacttype,
       coalesce(number_of_voters_in_ventile,0) as number_of_voters_in_ventile,
       coalesce(number_of_voters,0) as number_of_voters,

       coalesce(number_of_voters_attempted, 0) as unique_attempts,
       coalesce(all_attempts_in_pass_period, 0) as all_attempts,
       round(coalesce(1.0*unique_attempts/nullif(number_of_voters,0),0),4) as precent_attempted,
       round(coalesce(1.0*unique_attempts/nullif(all_attempts,0),0),4) as percent_unique_attempts,

       coalesce(activists_in_ventile,0) as activists_in_ventile,
       coalesce(activists,0) as activists,

       round(coalesce(1.0*ccj_1/nullif(ccj_all,0),0),4) as ccj_1rate,
       coalesce(ccj_1,0) as ccj_1,
       coalesce(ccj_2,0) as ccj_2,
       coalesce(ccj_3,0) as ccj_3,
       coalesce(ccj_4,0) as ccj_4,
       coalesce(ccj_5,0) as ccj_5,
       coalesce(ccj_all,0) as ccj_all,

       coalesce(ccj_negativeresult,0) as ccj_negativeresult,
       coalesce(total_contacts,0) as all_time_contacts
from (
    select state_code,
           collected_after_list_pass,
           contacttype,
           count(distinct case when attempted = 1 then person_id end) number_of_voters_attempted,
           count(distinct person_id) as number_of_voters,

           count(distinct case when contactdate >= pass_date then contactcontact_id end) all_attempts_in_pass_period,

           count(distinct case when activist_flag = 1
               OR activist_household_flag = 1
               OR donor_1plus_flag = 1
               OR donor_1plus_household_flag = 1 then person_id end) as activists,
           sum(ccj_id_1) as ccj_1,
           sum(ccj_id_2) as ccj_2,
           sum(ccj_id_3) as ccj_3,
           sum(ccj_id_4) as ccj_4,
           sum(ccj_id_5) as ccj_5,
           sum(ccj_id_1_2_3_4_5) as ccj_all,

           sum(ccj_contact_made) as total_contacts,
           sum(ccj_negative_result) as ccj_negativeresult
    from gotv_universes.in_field_validation_breakdown
    group by 1,2,3
    ) x
    left join (
        select state_code,
               count(distinct person_id) number_of_voters_in_ventile,
               count(distinct case when activist_flag = 1
                                              OR activist_household_flag = 1
                                              OR donor_1plus_flag = 1
                                              OR donor_1plus_household_flag = 1 then person_id end)  activists_in_ventile


        from gotv_universes.in_field_validation_breakdown
        group by 1
        ) t on x.state_code = t.state_code
order by 1,2,3)")

query_status <- query_civis(x=sql(state_aggregation_sql), database = "Bernie 2020")
