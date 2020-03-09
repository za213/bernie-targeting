
INPUT_TABLE <- Sys.getenv('INPUT_TABLE')
OUTPUT_TABLE <- Sys.getenv('OUTPUT_TABLE')
MATCHES_PER_ID <- Sys.getenv('MATCHES_PER_ID')
ENABLE_CASS <- Sys.getenv('ENABLE_CASS')
CUTOFF_THRESHOLD <- Sys.getenv('CUTOFF_THRESHOLD')
REMATCH_THRESHOLD <- Sys.getevn('REMATCH_THRESHOLD')
#INPUT_COLUMN_MAPPING <- Sys.getenv('INPUT_COLUMN_MAPPING')

library(rjson)
library(stringr)
library(civis)
library(tidyverse)

