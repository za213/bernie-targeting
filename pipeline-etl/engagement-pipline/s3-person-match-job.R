
library(civis)

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
                                                input_table = list(databaseName = 'Bernie 2020',
                                                                   schema = 'bernie_nmarchio2',
                                                                   table = 'events_users_clean'),
                                                output_table = list(databaseName = 'Bernie 2020',
                                                                    schema = 'bernie_nmarchio2',
                                                                    table = 'events_users_matched'),
                                                max_matches = 0,
                                                threshold = 0)

enhancements_post_civis_data_match_runs(id = 55863227)
