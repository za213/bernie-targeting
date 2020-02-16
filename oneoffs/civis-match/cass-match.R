
library(civis)

# Submit the Person Match Job
match_object <- match_targets_list()[[1]]$id # Set Match Object to Default Target Smart Universe
match_job <- enhancements_post_civis_data_match(name = 'Match Job',
                                                input_field_mapping = list(primary_key='sos_id',
                                                                           first_name='first_name',
                                                                           middle_name='last_name',
                                                                           #last_name='',
                                                                           #gender='',
                                                                           #phone='',
                                                                           #email='',
                                                                           #birth_date='',
                                                                           #birth_year='',
                                                                           #birth_month='',
                                                                           #birth_day='',
                                                                           #house_number='',
                                                                           #street='',
                                                                           #unit='',
                                                                           full_address='address1',
                                                                           city='city',
                                                                           state_code='state_code',
                                                                           zip='zip',
                                                                           #lat='',
                                                                           #lon=''
                                                                           ),
                                                match_target_id = match_object,
                                                parent_id = NULL,
                                                input_table = list(databaseName = 'Bernie 2020',
                                                                   schema = 'bernie_zasman',
                                                                   table = 'unmatched_sos'),
                                                output_table = list(databaseName = 'Bernie 2020',
                                                                    schema = 'bernie_zasman',
                                                                    table = 'unmatched_sos_matched'),
                                                max_matches = 1,
                                                threshold = 0)

match_job_run <- enhancements_post_civis_data_match_runs(id = match_job$id)

# Block until the Person Match job finishes
m <- await(f=enhancements_get_civis_data_match_runs, 
           id=match_job_run$civisDataMatchId,
           run_id=match_job_run$id)
get_status(m)
