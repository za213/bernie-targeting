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

final_query <- paste0("DROP TABLE if exists gotv_universes.in_field_validation_condensed;
CREATE TABLE gotv_universes.in_field_validation_condensed distkey(person_id) sortkey(person_id) as 
(SELECT distinct *
from (
    select lists.person_id
         ,case when lists.contacttype = 'spoke'
                        and apcd.cell_rank_for_person = 1 then 1
             when lists.contacttype = 'getthru_dialer'
                      and (apcd.phone_rank_for_person = 1 or apcd.cell_rank_for_person = 1) then 1
             when lists.contacttype = 'canvasses'
                      and (p.voting_address_id is not null
                               and p.myv_van_id is not null
                               and p.voting_street_address not ilike '%po box%'
                          ) then 1
             else 0 end valid_person
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
        left join (
            select person_id, phone_rank_for_person, cell_rank_for_person
            from bernie_data_commons.apcd_dnc
            where (phone_rank_for_person = 1 or cell_rank_for_person = 1)
            ) apcd on lists.person_id = apcd.person_id
        left join phoenix_analytics.person p on lists.person_id = p.person_id
    ) x);")

cat(final_query,file="sql.sql")

query_status <- query_civis(x=sql(final_query), database = "Bernie 2020")

state_aggregation_sql <- paste0("DROP TABLE if exists gotv_universes.in_field_validation_condensed_totals_state;
CREATE TABLE gotv_universes.in_field_validation_condensed_totals_state as 
(select state_code,
       coalesce(total_voters,0) as total_voters,
       round(coalesce(1.0*ccj_1/nullif(ccj_all,0),0),4) as total_1_rate,
       round(coalesce(1.0*ccj_1_pass/nullif(ccj_all_pass,0),0),4) as total_1_rate_pass,
       coalesce(number_of_voters_attempted, 0) as total_attempted,
       coalesce(number_of_voters_canvassed, 0) as total_canvassed,
       round(coalesce(1.0*total_attempted/nullif(total_voters,0),0),4) as percent_attempted,
       round(coalesce(1.0*total_canvassed/nullif(total_voters,0),0),4) as percent_canvassed,

       coalesce(total_doors,0) as total_doors,
       round(coalesce(1.0*doors_ccj_1/nullif(doors_ccj_all,0),0),4) as canvass_1_rate,
       coalesce(doors_attempted, 0) as doors_attempted,
       coalesce(doors_canvassed, 0) as doors_canvassed,
       round(coalesce(1.0*doors_attempted/nullif(total_doors,0),0),4) as percent_doors_attempted,
       round(coalesce(1.0*doors_canvassed/nullif(total_voters,0),0),4) as percent_doors_canvassed,

       coalesce(total_phones,0) as total_phones,
       round(coalesce(1.0*dialer_ccj_1/nullif(dialer_ccj_all,0),0),4) as phone_1_rate,
       coalesce(dialer_attempted, 0) as phones_attempted,
       coalesce(dialer_canvassed, 0) as phones_canvassed,
       round(coalesce(1.0*phones_attempted/nullif(total_phones,0),0),4) as percent_phones_attempted,
       round(coalesce(1.0*phones_canvassed/nullif(total_phones,0),0),4) as percent_phones_canvassed,

       coalesce(total_cells,0) as total_cells,
       round(coalesce(1.0*cells_ccj_1/nullif(cells_ccj_all,0),0),4) as cell_1_rate,
       coalesce(cells_attempted, 0) as cells_attempted,
       coalesce(cells_canvassed, 0) as cells_canvassed,
       round(coalesce(1.0*cells_attempted/nullif(total_cells,0),0),4) as percent_cells_attempted,
       round(coalesce(1.0*cells_canvassed/nullif(total_cells,0),0),4) as percent_cells_canvassed


from (
      select state_code, 
             count(distinct person_id) total_voters,
             sum(ccj_id_1) ccj_1,
             sum(ccj_id_1_2_3_4_5) as ccj_all,
             sum(case when contactdate > pass_date then ccj_id_1 else 0 end) ccj_1_pass,
             sum(case when contactdate > pass_date then ccj_id_1_2_3_4_5 else 0 end) as ccj_all_pass,
             count(distinct case when attempted = 1 then person_id end) number_of_voters_attempted,
             count(distinct case when canvassed = 1 then person_id end) number_of_voters_canvassed,

             count(distinct case when valid_person = 1 and contacttype = 'canvasses' then person_id end) total_doors,
             sum(case when contacttype = 'canvasses' and contactdate > pass_date then ccj_id_1 else 0 end) doors_ccj_1,
             sum(case when contacttype = 'canvasses' and contactdate > pass_date then ccj_id_1_2_3_4_5 else 0 end) as doors_ccj_all,
             count(distinct case when attempted = 1 and contacttype = 'canvasses' then person_id end) doors_attempted,
             count(distinct case when canvassed = 1 and contacttype = 'canvasses' then person_id end) doors_canvassed,

             count(distinct case when valid_person = 1 and contacttype = 'getthru_dialer' then person_id end) total_phones,
             sum(case when contacttype = 'getthru_dialer' and contactdate > pass_date then ccj_id_1 else 0 end) dialer_ccj_1,
             sum(case when contacttype = 'getthru_dialer' and contactdate > pass_date then ccj_id_1_2_3_4_5 else 0 end) as dialer_ccj_all,
             count(distinct case when attempted = 1 and contacttype = 'getthru_dialer' then person_id end) dialer_attempted,
             count(distinct case when canvassed = 1 and contacttype = 'getthru_dialer' then person_id end) dialer_canvassed,

             count(distinct case when valid_person = 1 and contacttype = 'spoke' then person_id end) total_cells,
             sum(case when contacttype = 'spoke' and contactdate > pass_date then ccj_id_1 else 0 end) cells_ccj_1,
             sum(case when contacttype = 'spoke' and contactdate > pass_date then ccj_id_1_2_3_4_5 else 0 end) as cells_ccj_all,
             count(distinct case when attempted = 1 and contacttype = 'spoke' then person_id end) cells_attempted,
             count(distinct case when canvassed = 1 and contacttype = 'spoke' then person_id end) cells_canvassed

      from gotv_universes.in_field_validation_condensed
      group by 1
  ) x
order by 1)")

query_status <- query_civis(x=sql(state_aggregation_sql), database = "Bernie 2020")
