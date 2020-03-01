# Container: https://platform.civisanalytics.com/spa/#/scripts/containers/65101602

library(civis)
library(tidyverse)

# Parameters --------------------------------------------------------------
matches_per_id = 3 # integer, number of matches allowed per source ID (will be deduplicated in output table)
rematch_threshold = .6 # decimal, rematch all records less than this match score on each update (automatically includes new records without scores)
cutoff_threshold = .4 # decimal, keep all matches greater than or equal to this match score in final table

# Source table and schema
# Can be an partial or complete source table (records already in destination table and above match threshold will be excluded from matching)
input_table_param = list(schema = 'bernie_nmarchio2',
                         table = 'ak_for_matching_test')

# input_table_param <- Sys.getenv("INPUT_SCHEMA_TABLE_LIST")

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

# input_table_param <- Sys.getenv("COLUMN_MAPPING")

# Destination table and schema
# If this table already exists it will be unioned and deduplicated into the updated output table
output_table_param = list(schema = 'bernie_nmarchio2',
                          table = 'ak_civis_match_out')

# output_table_param <- Sys.getenv("OUTPUT_SCHEMA_TABLE_LIST")

# Functions ---------------------------------------------------------------

dedupe_match_table <- function(input_schema_table = NULL,
                               match_schema_table = NULL,
                               output_schema_table = NULL,
                               prefer_state_matches = TRUE,
                               cutoff_param = 0){
        sql_pii <- c()
        state_must_match = ''
        state_sort = ''
        for (i in names(compact(pii_param))) {
                v = paste0('\n,',compact(pii_param)[[i]],'')
                if (i == "state_code" & prefer_state_matches = TRUE) {
                        state_must_match = paste0("\n, case when input.",compact(pii_param)[[i]]," is not null and (input.",compact(pii_param)[[i]]," = pxpa.state_code or input.",compact(pii_param)[[i]]," = mxts.state_code) then 1 else 0 end as state_match ")
                        state_sort = 'state_match desc, '
                }
                sql_pii<- c(sql_pii,v)
        }
        
        sql_query_xwalk <- c()
        sql_query_xwalk <- paste0("(select person_id, matched_id as voterbase_id, score , getdate()::date as matched_date ",paste0(sql_pii,collapse='')," from 
        (select * , row_number() over(partition by source_id order by ",state_sort," score desc) as best_record_rank
        from (select coalesce(pxpa.person_id, mxts.person_id) as person_id,match.source_id,match.matched_id,match.score ",paste0(sql_pii,collapse=''),state_must_match," 
        from (select * from ",match_schema_table," where score >= ",cutoff_param,") match
        left join ",input_schema_table," input on match.source_id = input.",pii_param$primary_key,"
        left join (select person_id , voterbase_id , state_code from phoenix_analytics.person) pxpa on match.matched_id = pxpa.voterbase_id
        left join (select person_id, voterbase_id, state_code from bernie_data_commons.master_xwalk_ts) mxts on match.matched_id = mxts.voterbase_id ) ) where best_record_rank = 1 and (person_id is not null or voterbase_id is not null))")
        
        match_output_sql <- paste0('DROP TABLE IF EXISTS ',output_schema_table,"; 
                              CREATE TABLE ",output_schema_table,' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') AS ',sql_query_xwalk,';') 
        cat(match_output_sql,file="sql.sql")
        
        match_output_status <- civis::query_civis(x=sql(match_output_sql), database = 'Bernie 2020') 
        
        return(match_output_status)
}

# Drop staging tables if they exist ---------------------------------------

drop_tables_sql <- paste0('\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),';
\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_1_match1'),';
\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_2_fullmatch'),';
\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch'),';
\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_4_cass'),';
\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),';
\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_6_match2'),';
\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_7_fullmatch'),';')
drop_table_status <- civis::query_civis(x=sql(drop_tables_sql), database = 'Bernie 2020')

# Setup Initial Table ----------------------------------------------------

# Check if final table exists and if so take records below rematch threshold from previous run
cass_row_count <- civis::read_civis(x=sql(paste0('select count(*) from ',input_table_param$schema,'.',input_table_param$table)), database = 'Bernie 2020') 
parallel_chunks <- ceiling((cass_row_count$count/250000)/4)
check_if_final_table_exists <- civis::read_civis(x=sql(paste0("select count(*) from information_schema.tables where table_schema = '",output_table_param$schema,"' and table_name = '",output_table_param$table,"';")), database = 'Bernie 2020')
if (check_if_final_table_exists$count == 1) {
        match_universe <- paste0("(select *, ntile(",parallel_chunks,") over (order by random()) as chunk from ",paste0(output_table_param$schema,'.',input_table_param$table)," 
                                 left join (select ",pii_param$primary_key,", score from ",output_table_param$schema,'.',output_table_param$table,") using(",pii_param$primary_key,") where score < ",rematch_threshold," or score is null)")
} else {
        match_universe <- paste0("(select *, ntile(",parallel_chunks,") over (order by random()) as chunk from ",paste0(output_table_param$schema,'.',input_table_param$table),")")
}
# Create initial staging table based on above checks
input_table_sql <- paste0('drop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),';
                          create table ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),
                          ' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as ',match_universe,';') 
input_table_status <- civis::query_civis(x=sql(input_table_sql), database = 'Bernie 2020') 

# Person Match  -----------------------------------------------------------

# Submit the person match job
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

# Block until the match job finishes
m <- await(f=enhancements_get_civis_data_match_runs, 
           id=match_job_run_civis$civisDataMatchId,
           run_id=match_job_run_civis$id)
get_status(m)

# Best matches from first run
deduped_status <- dedupe_match_table(input_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),
                                     match_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_1_match1'),
                                     output_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_2_fullmatch'),
                                     prefer_state_matches = FALSE,
                                     cutoff_param = 0)
deduped_status 

# Create table of below threshold matches to run through CASS and rematch again
rematch_table_sql <- paste0('create table ',output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch as 
                            (select input0.* from ',output_table_param$schema,'.',input_table_param$table,'_stage_0_input input0 
                            left join 
                            (select * from ',output_table_param$schema,'.',input_table_param$table,'_stage_2_fullmatch where score >= ',rematch_threshold,') input2 using(',pii_param$primary_key,') 
                            where input2.',pii_param$primary_key,' is null);')
rematch_table_status <- civis::query_civis(x=sql(paste0(rematch_table_sql)), database = 'Bernie 2020') 
rematch_table_status

# CASS Address Standardization --------------------------------------------

# Submit CASS jobs in parallel
chunk_jobs <- c()
for (chunk_i in parallel_chunks) {
        clean_job <- civis::enhancements_post_cass_ncoa(name = paste0('CASS Job Chunk ',chunk_i,': ',input_table_param$schema,'.',input_table_param$table), 
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
        chunk_jobs <- c(chunk_jobs,clean_job_run$id)
}

# Capture successful CASS jobs
chunk_df <- data.frame(parallel_chunks,chunk_jobs)
chunk_successes <- c()
for (i in unique(chunk_df$parallel_chunks)) {
        r <- await(f=enhancements_get_cass_ncoa_runs, 
                   id=clean_job_run$cassNcoaId, 
                   run_id=chunk_df$chunk_jobs[i])
        if (get_status(r) == "succeeded") {
                chunk_success <- parallel_chunks[i]
                chunk_successes <- c(chunk_successes, chunk_success)
        }
}

# Union chunked CASS jobs together and then drop staging tables
cass_union_sql <- c()
cass_drop_sql <- c()
for (chunk_i in chunk_successes) {
        u <- paste0("(select * from ",output_table_param$schema,'.',input_table_param$table,'_stage_4_cass_',chunk_i,')')
        cass_union_sql <- c(cass_union_sql, u)
        d <- paste0("\n drop table if exists ",output_table_param$schema,'.',input_table_param$table,'_stage_4_cass_',chunk_i,'; ')
        cass_drop_sql <- c(cass_drop_sql, d)
}

cass_union_sql <- paste0('create table ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_4_cass'),' as (select * from ',paste(cass_union_sql, collapse = ' union all '),');')
cat(cass_union_sql ,file="sql.sql")
cass_union_status <- civis::query_civis(x=sql(cass_union_sql), database = 'Bernie 2020') 
cass_union_status

cass_drop_sql <- paste(cass_drop_sql, collapse = ' ')
cat(cass_drop_sql, file="sql.sql")
cass_drop_status <- civis::query_civis(x=sql(cass_drop_sql), database = 'Bernie 2020') 
cass_drop_status

# Coalesce CASS table with input table's PII
coalesce_columns_sql <- c()
for (i in names(compact(pii_param))) {
        v = paste0('input.',compact(pii_param)[[i]],'')
        if (i == "first_name") {
                v = paste0("coalesce(name.first_name_guess, input.",compact(pii_param)[[i]],") as ",compact(pii_param)[[i]])
        }
        if (i == "last_name") {
                v = paste0("coalesce(name.last_name_guess, input.",compact(pii_param)[[i]],") as ",compact(pii_param)[[i]])
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
        coalesce_columns_sql <- c(coalesce_columns_sql,v)
}

# Guess first_name and last_name from email (or if last name is empty and two words are in first name field)
if ((is.null(pii_param$first_name) && is.null(pii_param$last_name) && is.null(pii_param$email)) == FALSE) {
        clean_name_sql <- paste0(" left join (select ",pii_param$primary_key,", first_name_guess, last_name_guess 
                                         from (select ",pii_param$primary_key,"
                                        ,nullif(initcap(SPLIT_PART(regexp_replace(lower(first_name),' and | & ',';'), ';', 1)),'') as first_name_partner_1
                                        ,nullif(initcap(SPLIT_PART(regexp_replace(lower(first_name),' and | & ',';'), ';', 2)),'') as first_name_partner_2
                                        ,nullif(regexp_replace(lower(LEFT(email, CHARINDEX('@',email)-1)), '[^a-zA-Z\\.\\_]',''),'') as email_preparse
                                        ,nullif(initcap(REGEXP_SUBSTR( email_preparse, '^([^._]+)')),'') as first_from_email
                                        ,nullif(initcap(REGEXP_SUBSTR( email_preparse, '[^._]*$')),'') as last_from_email
                                        ,nullif(initcap(left(first_name, CHARINDEX(' ', first_name))),'') as first_from_first
                                        ,nullif(initcap(substring(first_name, CHARINDEX(' ', first_name)+1, len(first_name)-(CHARINDEX(' ', first_name)-1))),'') as last_from_first
                                        ,case when last_from_first = first_name then NULL else last_from_first end as last_from_first_2
                                        ,case when (first_name is not null and first_name_partner_2 is not null) and first_name_partner_1 <> '' then first_name_partner_1
                                              when (first_name is not null and last_name is null and last_from_first_2 is not null) and first_from_first <> '' then first_from_first
                                              when (first_name is null and last_from_email is not null) and (first_from_email <> last_from_email ) and first_from_email <> '' then first_from_email
                                              else initcap(first_name) end as first_name_guess
                                        ,case 
                                              when last_name is null then nullif(coalesce(last_from_first_2, last_from_email),'') 
                                              else initcap(last_name) end as last_name_guess
                                         from ",output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch)) name using(',pii_param$primary_key,')')
} else {
        clean_name_sql <- paste0(" left join (select ",pii_param$primary_key,", NULL as first_name_guess, NULL as last_name_guess 
                                         from ",output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch)) name using(',pii_param$primary_key,')')
}

coalesce_table_sql <- paste0('\n (select ',paste(coalesce_columns_sql, collapse = '\n, '),' 
                                      \n from ',output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch input',' 
                                      \n ',clean_name_sql,' 
                                      \n left join ',output_table_param$schema,'.',input_table_param$table,'_stage_4_cass cass',' 
                                      \n using(',pii_param$primary_key,') \nwhere ',pii_param$primary_key,' is not null)')

# Submit coalesce query to combine raw PII and successful CASS jobs
match_input_sql <- paste0('drop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),';
                          create table ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as ',coalesce_table_sql,';')  
cat(match_input_sql,file="sql.sql")
match_input_status <- civis::query_civis(x=sql(match_input_sql), database = 'Bernie 2020') 
match_input_status

# Person Match --------------------------------------------------------

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

# Find Best Records --------------------------------------------------------

# Best matches from second run
deduped_status <- dedupe_match_table(input_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),
                                     match_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_6_match2'),
                                     output_schema_table = paste0(output_table_param$schema,'.',input_table_param$table,'_stage_7_fullmatch'),
                                     prefer_state_matches = TRUE,
                                     cutoff_param = 0)
deduped_status 


column_list <- paste0(' person_id , voterbase_id , score , matched_date , ',paste(as.vector(unlist(compact(pii_param))),collapse = ' , '))

if (check_if_final_table_exists$count == 1) {
        existing_universe <- paste0(" union all (select ",column_list," from ",output_table_param$schema,'.',output_table_param$table,")")
} else {
        existing_universe <- ''
}

# Matched table (all records)
complete_table_sql <- paste0('drop table if exists ',output_table_param$schema,'.',output_table_param$table,'_all_matches; 
                              create table ',output_table_param$schema,'.',output_table_param$table,'_all_matches distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as 
                              (select ',column_list,' from
                              (select *, row_number() over(partition by ',pii_param$primary_key,' order by score desc) as best_record_rank from 
                              (select * from
                              (select * from ',output_table_param$schema,'.',input_table_param$table,'_stage_2_fullmatch) 
                              union all 
                              (select * from ',output_table_param$schema,'.',input_table_param$table,'_stage_7_fullmatch)',existing_universe,')) where best_record_rank = 1);')

complete_table_status <- civis::query_civis(x=sql(complete_table_sql), database = 'Bernie 2020') 
complete_table_status

# Matched table (only records above cutoff_threshold)
final_table_sql <- paste0('drop table if exists ',output_table_param$schema,'.',output_table_param$table,'; 
                          create table ',output_table_param$schema,'.',output_table_param$table,' distkey(',pii_param$primary_key,') sortkey(',pii_param$primary_key,') as 
                          (select * from ',output_table_param$schema,'.',output_table_param$table,'_all_matches where score >= ',cutoff_threshold,');')

final_table_status <- civis::query_civis(x=sql(final_table_sql), database = 'Bernie 2020') 
final_table_status

#Drop staging tables
# drop_tables_sql <- paste0('\ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_0_input'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_1_match1'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_2_fullmatch'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_3_rematch'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_4_cass'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_5_coalesce'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_6_match2'),';
# \ndrop table if exists ',paste0(output_table_param$schema,'.',input_table_param$table,'_stage_7_fullmatch'),';')
# drop_table_status <- civis::query_civis(x=sql(drop_tables_sql), database = 'Bernie 2020')

#sink(file("match_console.log"), append=TRUE, type="message")
#cat(readLines("match_console.log"), sep="\n")
