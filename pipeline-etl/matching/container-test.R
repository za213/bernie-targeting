
library(civis)
library(tidyverse)

input_table_param = Sys.getenv('INPUT_TABLE')
matches_per_id = Sys.getenv('MATCHES_PER_ID')
enable_cass = Sys.getenv('ENABLE_CASS')  
rematch_threshold = Sys.getenv('REMATCH_THRESHOLD')
cutoff_threshold = Sys.getenv('CUTOFF_THRESHOLD') 
pii_param = Sys.getenv('INPUT_TABLE_PII')
output_table_param = Sys.getenv('OUTPUT_TABLE')

cat(matches_per_id)
cat(enable_cass)
cat(rematch_threshold)
cat(cutoff_threshold)
cat(input_table_param)
cat(pii_param )
cat(output_table_param)
