
# Parameters --------------------------------------------------------------

#enable_cass = TRUE # boolean, TRUE to run CASS address standardization (CASS not recommended for jobs over 2 million)
matches_per_id = 3 # integer, number of matches allowed per source ID (will be deduplicated in destination table)
rematch_threshold = .7 # decimal, rematch all records with scores below this threshold on each update
cutoff_threshold = .4 # decimal, keep all matches greater than or equal to this match score threshold in final table

# Source table and schema
input_table_param = list(schema = 'bernie_nmarchio2',
                         table = 'ak_for_matching')

# Source table columns
pii_param = list(primary_key='id',
                 first_name='first_name',
                 middle_name='middle_name',
                 last_name='last_name',
                 phone='phone_1',
                 email='email',
                 full_address='address1',
                 unit='address2',
                 city='city',
                 state_code='state',
                 zip='zip',
                 gender=NULL,
                 birth_date=NULL,
                 birth_year=NULL,
                 birth_month=NULL,
                 birth_day=NULL,
                 house_number=NULL,
                 street=NULL,
                 lat=NULL,
                 lon=NULL)

# Destination table and schema, columns: <primary_key>, person_id, voterbase_id, score, matched_date
output_table_param = list(schema = 'bernie_nmarchio2',
                          table = 'ak_civis_match')

# Functions ---------------------------------------------------------------

dedupe_match_table <- function(input_schema_table = NULL,
                               match_schema_table = NULL,
                               output_schema_table = NULL,
                               cutoff_param = rematch_threshold){
        sql_pii <- c()
        state_sort = ''
        for (i in names(compact(pii_param))) {
                v = paste0('\n,input.',compact(pii_param)[[i]],'')
                if (i == "state_code") {
                        v = paste0("\n,case when input.",compact(pii_param)[[i]]," = pxpa.state_code or input.",compact(pii_param)[[i]]," = mxts.state_code then 1 else 0 end as state_match")
                        state_sort = 'state_match desc, '
                }
                sql_pii<- c(sql_pii,v)
        }
        
        sql_query_xwalk <- c()
        sql_query_xwalk <- paste0("(select ",pii_param$primary_key," , person_id, matched_id as voterbase_id, score , getdate()::date as matched_date from 
        (select * , row_number() over(partition by source_id order by ",state_sort," score desc) as best_record_rank
        from (select coalesce(pxpa.person_id, mxts.person_id) as person_id,match.source_id,match.matched_id,match.score",paste0(sql_pii,collapse=''),"
        from (select * from ",match_schema_table," where score >= ",cutoff_param,") match
        left join ",input_schema_table," input on match.source_id = input.",pii_param$primary_key,"
        left join (select person_id , voterbase_id , state_code from phoenix_analytics.person) pxpa on match.matched_id = pxpa.voterbase_id
        left join (select person_id, voterbase_id, state_code from bernie_data_commons.master_xwalk_ts) mxts on match.matched_id = mxts.voterbase_id ) ) where best_record_rank = 1 and person_id is not null)")
        
        match_output_sql <- paste0('DROP TABLE IF EXISTS ',output_schema_table,"; set query_group to 'importers'; set wlm_query_slot_count to 2;
                                   CREATE TABLE ",output_schema_table,' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') AS ',sql_query_xwalk,';') 
        cat(match_output_sql,file="sql.sql")
        
        match_output_status <- civis::query_civis(x=sql(match_output_sql), database = 'Bernie 2020') 
        
        return(match_output_status)
}


# Packages ----------------------------------------------------------------

library(civis)
library(tidyverse)

# Build Input Table
cass_row_count <- civis::read_civis(x=sql(paste0('select count(*) from ',input_table_param$schema,'.',input_table_param$table)), database = 'Bernie 2020') 
parallel_chunks <- ceiling((cass_row_count$count/250000)/4)
check_if_final_table_exists <- civis::read_civis(x=sql(paste0("select count(*) from information_schema.tables where table_schema = '",output_table_param$schema,"' and table_name = '",output_table_param$table,"';")), database = 'Bernie 2020')
if (check_if_final_table_exists$count == 1) {
        match_universe <- paste0("(select *, ntile(",parallel_chunks,") over (order by random()) as chunk from ",paste0(input_table_param$schema,'.',input_table_param$table)," left join (select ",pii_param$primary_key,", score from ",output_table_param$schema,'.',output_table_param$table,") using(",pii_param$primary_key,") where score < ",rematch_threshold," or score is null)")
} else {
        match_universe <- paste0("(select *, ntile(",parallel_chunks,") over (order by random()) as chunk from ",paste0(input_table_param$schema,'.',input_table_param$table),")")
}
input_table_sql <- paste0('drop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),';
                          create table ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),
                          ' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as ',match_universe,';') 
input_table_status <- civis::query_civis(x=sql(input_table_sql), database = 'Bernie 2020') 

# Person Match  -----------------------------------------------------------

# Submit the Initial Person Match Job
match_job_civis <- civis::enhancements_post_civis_data_match(name = paste0('Civis Match Job 1: ',input_table_param$schema,'.',input_table_param$table),
                                                             input_field_mapping = compact(pii_param),
                                                             match_target_id = civis::match_targets_list()[[1]]$id, # Civis Voterfile = 1, DNC = 2
                                                             parent_id = NULL,
                                                             input_table = list(databaseName = 'Bernie 2020',
                                                                                schema = output_table_param$schema,
                                                                                table = paste0(input_table_param$table,'_stage_0_input')),
                                                             output_table = list(databaseName = 'Bernie 2020',
                                                                                 schema = output_table_param$schema,
                                                                                 table = paste0(input_table_param$table,'_stage_1_match1')),
                                                             max_matches = matches_per_id,
                                                             threshold = 0)
match_job_run_civis <- civis::enhancements_post_civis_data_match_runs(id = match_job_civis$id)

# Block until the Match jobs finish
m <- await(f=enhancements_get_civis_data_match_runs, 
           id=match_job_run_civis$civisDataMatchId,
           run_id=match_job_run_civis$id)
get_status(m)

# Collapse match table to best matches
deduped_status <- dedupe_match_table(input_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),
                                     match_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_1_match1'),
                                     output_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_2_bestmatch'),
                                     cutoff_param = rematch_threshold)
deduped_status 

# Save best matches and create separate table of weak matches for CASS 
if (check_if_final_table_exists$count == 1) {
        new_matches_sql <- paste0('insert into ',paste0(output_table_param$schema,'.',output_table_param$table),' (select * from ',output_table_param$schema,'.',input_table_param$table,'_stage_2_bestmatch);')
} else {
        new_matches_sql <- paste0("create table ",output_table_param$schema,'.',output_table_param$table," as (select * from ",output_table_param$schema,'.',input_table_param$table,'_stage_2_bestmatch);')
}
create_rematch_table_sql <- paste0('create table ',output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch as (select input0.* from ',output_table_param$schema,'.',input_table_param$table,'_stage_0_input input0 left join ',output_table_param$schema,'.',input_table_param$table,'_stage_2_bestmatch input2 using(',pii_param$primary_key,') where input2.',pii_param$primary_key,' is null);')
rematch_table_status <- civis::query_civis(x=sql(paste0(new_matches_sql,create_rematch_table_sql)), database = 'Bernie 2020') 
rematch_table_status

# CASS Address Standardization --------------------------------------------

# Run parallelized CASS jobs and coalesce with raw PII table

for (chunk_i in 1:parallel_chunks) {
        clean_job <- civis::enhancements_post_cass_ncoa(name = paste0('CASS Job Chunk ',chunk_i,': ',output_table_param$schema,'.',input_table_param$table), 
                                                        source = list(databaseTable = list(schema = output_table_param$schema,
                                                                                           table = paste0(input_table_param$table,'_stage_3_rematch'),
                                                                                           remoteHostId = get_database_id('Bernie 2020'),
                                                                                           credentialId = default_credential(),
                                                                                           multipartKey = list(pii_param$primary_key))),
                                                        destination = list(databaseTable = list(schema = output_table_param$schema,
                                                                                                table = paste0(input_table_param$table,'_stage_4_cass_',chunk_i))),
                                                        column_mapping = list(address1=pii_param$full_address,
                                                                              address2=pii_param$unit,
                                                                              city=pii_param$city,
                                                                              state=pii_param$state_code,
                                                                              zip=pii_param$zip#,name=paste0(pii_param$first_name,'+',pii_param$last_name)
                                                        ),
                                                        use_default_column_mapping = "false",
                                                        output_level = "cass",
                                                        limiting_sql = paste0(pii_param$primary_key,' is not null and chunk = ',chunk_i))
        clean_job_run <- enhancements_post_cass_ncoa_runs(clean_job$id)
}

# Block until the last CASS job finishes
r <- await(f=enhancements_get_cass_ncoa_runs, 
           id=clean_job_run$cassNcoaId, 
           run_id=clean_job_run$id)
get_status(r)

# Union CASS jobs together and then drop chunked tables
sql_cass_union <- c()
sql_cass_drop <- c()
for (chunk_i in 1:parallel_chunks) {
        u <- paste0("(select * from ",output_table_param$schema,'.',input_table_param$table,'_stage_4_cass_',chunk_i,')')
        sql_cass_union <- c(sql_cass_union, u)
        d <- paste0("drop table if exists ",output_table_param$schema,'.',input_table_param$table,'_stage_4_cass_',chunk_i,';')
        sql_cass_drop <- c(sql_cass_drop, d)
}
# Union tables
sql_cass_union <- paste0('create table ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_4_cass'),' as (select * from ',paste(sql_cass_union, collapse = ' union all '),');')
cat(sql_cass_union ,file="sql.sql")
cass_union_status <- civis::query_civis(x=sql(sql_cass_union), database = 'Bernie 2020') 
cass_union_status
# Drop tables
cat(sql_cass_drop, file="sql.sql")
cass_drop_status <- civis::query_civis(x=sql(sql_cass_drop), database = 'Bernie 2020') 
cass_drop_status
# Coalesce CASS table with input table's PII
sql_coalesce_columns <- c()
for (i in names(compact(pii_param))) {
        v = paste0('input.',compact(pii_param)[[i]],'')
        if (i == "first_name") {
                v = paste0("coalesce(input.",compact(pii_param)[[i]],", name.first_name_guess) as ",compact(pii_param)[[i]])
        }
        if (i == "last_name") {
                v = paste0("coalesce(input.",compact(pii_param)[[i]],", name.last_name_guess ) as ",compact(pii_param)[[i]])
        }
        if (i == "full_address") {
                v = paste0('coalesce(cass.std_priadr::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
        }
        if (i == "unit") {
                v = paste0('coalesce(cass.std_secadr::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
        }
        if (i == "city") {
                v = paste0('coalesce(cass.std_city::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
        }
        if (i == "state_code") {
                v = paste0('coalesce(cass.std_state::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
        }
        if (i == "zip") {
                v = paste0('coalesce(cass.std_zip::varchar(256) , input.',compact(pii_param)[[i]],'::varchar(256) ) AS ',compact(pii_param)[[i]])
        }
        sql_coalesce_columns <- c(sql_coalesce_columns,v)
}

# Guess names from first_name and email
if ((is.null(pii_param$first_name) && is.null(pii_param$last_name) && is.null(pii_param$email)) == FALSE) {
        clean_name_sql <- paste0(" left join (select ",pii_param$primary_key,", first_name_guess, last_name_guess 
                                         from (select ",pii_param$primary_key,"
                                        ,nullif(regexp_replace(lower(LEFT(email, CHARINDEX('@',email)-1)), '[^a-zA-Z\\.]',''),'') as email_preparse
                                        ,nullif(initcap(SPLIT_PART(email_preparse, '.', 1)),'') as first_from_email
                                        ,nullif(initcap(REGEXP_SUBSTR( email_preparse, '[^.]*$')),'') as last_from_email
                                        ,nullif(left(first_name, CHARINDEX(' ', first_name)),'') as first_from_first
                                        ,nullif(substring(first_name, CHARINDEX(' ', first_name)+1, len(first_name)-(CHARINDEX(' ', first_name)-1)),'') as last_from_first
                                        ,case when last_from_first = first_name then NULL else last_from_first end as last_from_first_2
                                        ,nullif(coalesce(first_from_first, first_name, first_from_email),'') as first_name_guess
                                        ,nullif(coalesce(last_name, last_from_first_2, last_from_email, first_from_email, first_name_guess),'') as last_name_coalesced
                                        ,case when last_name_coalesced = first_name then NULL else last_name_coalesced end as last_name_guess
                                         from ",output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch)) name using(',pii_param$primary_key,')')
} else {
        clean_name_sql <- paste0(" left join (select ",pii_param$primary_key,", NULL as first_name_guess, NULL as last_name_guess 
                                         from ",output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch)) name using(',pii_param$primary_key,')')
}

sql_coalesce_table <- paste0('\n (select ',paste(sql_coalesce_columns, collapse = '\n, '),' 
                                      \n from ',output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch input',' 
                                      \n ',clean_name_sql,' 
                                      \n left join ',output_table_param$schema,'.',input_table_param$table,'_stage_4_cass cass',' 
                                      \n using(',pii_param$primary_key,') \nwhere ',pii_param$primary_key,' is not null)')

# Submit coalesce query
match_input_sql <- paste0('drop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),';
                          create table ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as ',sql_coalesce_table,';')  
cat(match_input_sql,file="sql.sql")
match_input_status <- civis::query_civis(x=sql(match_input_sql), database = 'Bernie 2020') 
match_input_status

# Civis Person Match --------------------------------------------------------

# Submit the Person Match Job
match_job_civis <- civis::enhancements_post_civis_data_match(name = paste0('Civis Match Job 2: ',input_table_param$schema,'.',input_table_param$table),
                                                             input_field_mapping = compact(pii_param),
                                                             match_target_id = civis::match_targets_list()[[1]]$id, # Civis Voterfile = 1, DNC = 2
                                                             parent_id = NULL,
                                                             input_table = list(databaseName = 'Bernie 2020',
                                                                                schema = output_table_param$schema,
                                                                                table = paste0(input_table_param$table,'_stage_5_coalesce')),
                                                             output_table = list(databaseName = 'Bernie 2020',
                                                                                 schema = output_table_param$schema,
                                                                                 table = paste0(input_table_param$table,'_stage_6_match2')),
                                                             max_matches = matches_per_id,
                                                             threshold = 0)
match_job_run_civis <- civis::enhancements_post_civis_data_match_runs(id = match_job_civis$id)

# Block until the Match jobs finish
m <- await(f=enhancements_get_civis_data_match_runs, 
           id=match_job_run_civis$civisDataMatchId,
           run_id=match_job_run_civis$id)
get_status(m)

# Find Best Record --------------------------------------------------------

# Collapse match table to best matches
deduped_status <- dedupe_match_table(input_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),
                                     match_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_6_match2'),
                                     output_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_7_bestmatch'),
                                     cutoff_param = cutoff_threshold)
deduped_status 

# Insert matched into final table 
new_matches_sql <- paste0('insert into ',paste0(output_table_param$schema,'.',output_table_param$table),' (select * from ',output_table_param$schema,'.',input_table_param$table,'_stage_7_bestmatch);')
match_table_status <- civis::query_civis(x=sql(new_matches_sql), database = 'Bernie 2020') 
match_table_status


# Drop staging tables
# drop_tables_sql <- paste0('drop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_1_match1'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_2_bestmatch'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_4_cass'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_6_match2'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_7_bestmatch'),';')
# drop_table_status <- civis::query_civis(x=sql(drop_tables_sql), database = 'Bernie 2020') 

