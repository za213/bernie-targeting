library(rjson)
library(stringr)
library(civis)
library(tidyverse)

# Input table parameters
input_table_param <- stringr::str_split(Sys.getenv('INPUT_TABLE'), '[.]', simplify=TRUE)
input_schema = input_table_param[1] 
input_table = input_table_param[2] 

# Output table parameters
output_table_param <- stringr::str_split(Sys.getenv('OUTPUT_TABLE'), '[.]', simplify=TRUE)
output_schema = output_table_param[1]
output_table = output_table_param[2]

# Matching parameters
matches_per_id = as.integer(Sys.getenv('MATCHES_PER_ID')) # integer, number of matches allowed per source ID (will be deduplicated in output table)
enable_cass = as.logical(Sys.getenv('ENABLE_CASS')) # boolean, run CASS address standardization
rematch_threshold = as.numeric(Sys.getenv('REMATCH_THRESHOLD')) # decimal, rematch all records less than this match score on each update (automatically includes new records without scores in input table)
cutoff_threshold =  as.numeric(Sys.getenv('CUTOFF_THRESHOLD')) # decimal, keep all matches greater than or equal to this match score in final table
pii_param <- rjson::fromJSON(Sys.getenv('INPUT_COLUMN_MAPPING'), simplify=FALSE)[1][[1]] # Source table columns

# Assert input params
stopifnot(matches_per_id >= 1 & matches_per_id <= 10) 
stopifnot(enable_cass == TRUE | enable_cass == FALSE) 
stopifnot(rematch_threshold >= 0 & rematch_threshold < 1) 
stopifnot(cutoff_threshold  >= 0 & cutoff_threshold  < 1 & cutoff_threshold < rematch_threshold) 
input_assert_df <- civis::read_civis(x=sql(paste0("select column_name, t.table_schema , t.table_name from information_schema.tables t inner join information_schema.columns c on c.table_name = t.table_name and c.table_schema = t.table_schema where (t.table_schema = '",input_schema,"' and t.table_name = '",input_table,"') order by ordinal_position")), database = 'Bernie 2020')
input_col_list <- unique(input_assert_df$column_name)
param_col_list <- unlist(compact(pii_param), use.names=FALSE)
stopifnot(param_col_list %in% input_col_list)

