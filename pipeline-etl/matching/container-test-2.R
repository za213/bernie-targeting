
INPUT_TABLE <- Sys.getenv('INPUT_TABLE')
OUTPUT_TABLE <- Sys.getenv('OUTPUT_TABLE')
ENABLE_CASS <- Sys.getenv('ENABLE_CASS')
MATCHES_PER_ID <- Sys.getenv('MATCHES_PER_ID')
CUTOFF_THRESHOLD <- Sys.getenv('CUTOFF_THRESHOLD')
REMATCH_THRESHOLD <- Sys.getenv('REMATCH_THRESHOLD')

primary_key_arg <- Sys.getenv('primary_key')
first_name_arg <- Sys.getenv('first_name')
middle_name_arg <- Sys.getenv('middle_name')
last_name_arg <- Sys.getenv('last_name')
phone_arg <- Sys.getenv('phone')
email_arg <- Sys.getenv('email')
full_address_arg <- Sys.getenv('full_address')
unit_arg <- Sys.getenv('unit')
city_arg <- Sys.getenv('city')
state_code_arg <- Sys.getenv('state_code')
zip_arg <- Sys.getenv('zip')
gender_arg <- Sys.getenv('gender')
birth_date_arg <- Sys.getenv('birth_date')

pii_param <- list(primary_key = primary_key_arg
                  first_name = first_name_arg
                  middle_name = middle_name_arg
                  last_name = last_name_arg
                  phone = phone_arg
                  email = email_arg
                  full_address = full_address_arg
                  unit = unit_arg
                  city = city_arg
                  state_code = state_code_arg
                  zip = zip_arg
                  gender = gender_arg
                  birth_date = birth_date_arg)

library(rjson)
library(stringr)
library(civis)
library(tidyverse)

# Input table parameters
input_table_param <- stringr::str_split(INPUT_TABLE, '[.]', simplify=TRUE)
input_schema = input_table_param[1] 
input_table = input_table_param[2] 

# Output table parameters
output_table_param <- stringr::str_split(OUTPUT_TABLE, '[.]', simplify=TRUE)
output_schema = output_table_param[1]
output_table = output_table_param[2]

# Matching parameters
matches_per_id = as.integer(MATCHES_PER_ID) # integer, number of matches allowed per source ID (will be deduplicated in output table)
enable_cass = as.logical(ENABLE_CASS) # boolean, run CASS address standardization
rematch_threshold = as.numeric(REMATCH_THRESHOLD) # decimal, rematch all records less than this match score on each update (automatically includes new records without scores in input table)
cutoff_threshold =  as.numeric(CUTOFF_THRESHOLD) # decimal, keep all matches greater than or equal to this match score in final table

#pii_param <- eval(parse(text=INPUT_COLUMN_MAPPING)) # rjson::fromJSON(INPUT_COLUMN_MAPPING, simplify=FALSE)[1][[1]] # Source table columns

# Assert input params
stopifnot(matches_per_id >= 1 & matches_per_id <= 10) 
stopifnot(enable_cass == TRUE | enable_cass == FALSE) 
stopifnot(rematch_threshold >= 0 & rematch_threshold < 1) 
stopifnot(cutoff_threshold  >= 0 & cutoff_threshold  < 1 & cutoff_threshold < rematch_threshold) 
input_assert_df <- civis::read_civis(x=sql(paste0("select column_name, t.table_schema , t.table_name from information_schema.tables t inner join information_schema.columns c on c.table_name = t.table_name and c.table_schema = t.table_schema where (t.table_schema = '",input_schema,"' and t.table_name = '",input_table,"') order by ordinal_position")), database = 'Bernie 2020')
input_col_list <- unique(input_assert_df$column_name)
param_col_list <- unlist(compact(pii_param), use.names=FALSE)
stopifnot(param_col_list %in% input_col_list)

