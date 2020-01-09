
library(civis)

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

print(clean_job)

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

