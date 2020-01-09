
library(civis)

# Drop clean tables
drop_table_sql = 'drop table if exists bernie_nmarchio2.events_details_clean; 
drop table if exists bernie_nmarchio2.events_users_clean;'
drop_status <- civis::query_civis(x=sql(drop_table_sql), database = 'Bernie 2020') 
drop_status

# Run CASS job on events table
clean_job_1 <- civis::enhancements_post_cass_ncoa(name ='Engagement Events CASS Job', 
                                                  source = list(databaseTable = list(schema = 'bernie_nmarchio2',
                                                                                     table = 'events_details',
                                                                                     remoteHostId = get_database_id('Bernie 2020'),     
                                                                                     credentialId = default_credential(),
                                                                                     multipartKey = list('unique_id'))),
                                                  destination = list(databaseTable = list(schema = 'bernie_nmarchio2',
                                                                                          table = 'events_details_clean')),
                                                  column_mapping = list(address1 = 'event_address1',
                                                                        address2 = 'event_address2',
                                                                        city = 'event_city',
                                                                        state = 'event_state',
                                                                        zip = 'event_zip',
                                                                        name = NULL,
                                                                        company = NULL),
                                                  use_default_column_mapping = "false",
                                                  output_level = "cass",
                                                  limiting_sql = "")

clean_job_1_run <- enhancements_post_cass_ncoa_runs(clean_job_1$id)

# Run CASS job on users table
clean_job_2 <- civis::enhancements_post_cass_ncoa(name ='Engagement Users CASS Job', 
                                                  source = list(databaseTable = list(schema = 'bernie_nmarchio2',
                                                                                     table = 'events_users',
                                                                                     remoteHostId = get_database_id('Bernie 2020'),
                                                                                     credentialId = default_credential(),
                                                                                     multipartKey = list('unique_id'))),
                                                  destination = list(databaseTable = list(schema = 'bernie_nmarchio2',
                                                                                          table = 'events_users_clean')),
                                                  column_mapping = list(address1 = 'user_address_line_1',
                                                                        address2 = 'user_address_line_2',
                                                                        city = 'user_city',
                                                                        state = 'user_state',
                                                                        zip = 'user_zip',
                                                                        name = NULL,
                                                                        company = NULL),
                                                  use_default_column_mapping = "false",
                                                  output_level = "cass",
                                                  limiting_sql = "person_id is null")

clean_job_2_run <- enhancements_post_cass_ncoa_runs(clean_job_2$id)

# Block until the users CASS job finishes
r <- await(f=enhancements_get_cass_ncoa_runs, 
           id=clean_job_2_run$cassNcoaId, 
           run_id=clean_job_2_run$id)
get_status(r)

# Block until the events CASS job finishes
r <- await(f=enhancements_get_cass_ncoa_runs, 
           id=clean_job_1_run$cassNcoaId, 
           run_id=clean_job_1_run$id)
get_status(r)

# Submit coalesce query
match_input_sql <- 'DROP TABLE IF EXISTS bernie_nmarchio2.events_users_match_input;
CREATE TABLE bernie_nmarchio2.events_users_match_input AS
(SELECT unique_id ,
       person_id ,
       source_data ,
       user_id ,
       user_email ,
       user_firstname ,
       user_lastname ,
       coalesce(std_priadr::varchar(256) ,user_address_line_1::varchar(256) ) AS user_address_line_1 ,
       coalesce(std_secadr::varchar(256) ,user_address_line_2::varchar(256) ) AS user_address_line_2 ,
       coalesce(std_city::varchar(256) ,user_city::varchar(256) ) AS user_city ,
       coalesce(std_state::varchar(256) ,user_state::varchar(256) ) AS user_state ,
       coalesce(std_zip::varchar(256) ,user_zip::varchar(256) ) AS user_zip ,
       user_phone ,
       user_modified_date ,
       user_address_latitude ,
       user_address_longitude
FROM bernie_nmarchio2.events_users
LEFT JOIN bernie_nmarchio2.events_users_clean using(unique_id) where person_id is null)'
match_input_status <- civis::query_civis(x=sql(match_input_sql), database = 'Bernie 2020') 
match_input_status

# Submit the Person Match Job
match_job <- enhancements_post_civis_data_match(name = 'Engagement Users Match Job',
                                                input_field_mapping = list(primary_key = 'unique_id',
                                                                           first_name = 'user_firstname',
                                                                           last_name = 'user_lastname',
                                                                           phone = 'user_phone',
                                                                           email = 'user_email',
                                                                           full_address = 'user_address_line_1',
                                                                           unit = 'user_address_line_2',
                                                                           city = 'user_city',
                                                                           state_code = 'user_state',
                                                                           zip = 'user_zip'),
                                                match_target_id = 55862728,
                                                parent_id = NULL,
                                                input_table = list(databaseName = 'Bernie 2020',
                                                                   schema = 'bernie_nmarchio2',
                                                                   table = 'events_users_match_input'),
                                                output_table = list(databaseName = 'Bernie 2020',
                                                                    schema = 'bernie_nmarchio2',
                                                                    table = 'events_users_match_output'),
                                                max_matches = 0,
                                                threshold = 0)

match_job_run <- enhancements_post_civis_data_match_runs(id = match_job$id)

# Block until the Person Match job finishes
m <- await(f=enhancements_get_civis_data_match_runs, 
           id=match_job_run$civisDataMatchId,
           run_id=match_job_run$id)
get_status(m)

# geocode_job <- enhancements_post_geocode(name = 'Geocode Job',
#                                          remote_host_id= get_database_id('Bernie 2020'),
#                                          credential_id = default_credential(),
#                                          source_schema_and_table = 'bernie_nmarchio2.events_details_clean',
#                                          multipart_key =  list('unique_id'),
#                                          limiting_sql = '',
#                                          target_schema = 'bernie_nmarchio2',
#                                          target_table = 'events_details_clean_geocoded',
#                                          country = 'us',
#                                          provider ='postgis',
#                                          output_address = TRUE)
