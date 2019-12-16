library(civis)
library(mice)
library(tidyverse)

# County
county_table <- read_civis(x = 'bernie_nmarchio2.geotable_intermed_county', database = "Bernie 2020")
dropcol <- c("census_county_fips","cty","county_name","statename","county_fips_2000",'gini_bottom_99pct','w2_pos_30_q5_f','w2_pos_30_q5_m')
county_table2 <- county_table %>% select(-one_of(dropcol)) 
county_nona<- na.omit(county_table2)
county_table2 <- county_table2 %>% 
  mutate(no_college = ifelse(is.na(college_tuition), 1, 0),
         college_tuition = ifelse(is.na(college_tuition), 0, college_tuition),
         college_graduation_rate_income_adjusted  = ifelse(is.na(college_graduation_rate_income_adjusted ), 0, college_graduation_rate_income_adjusted))
         
# Classification and regression trees imputation
sapply(county_table2, function(x) sum(is.na(x)))
imputed_data <- mice(county_table2, m=1, maxit = 1, method = "cart", seed = 500)
complete_data <- mice::complete(imputed_data,1)
sapply(complete_data, function(x) sum(is.na(x)))

# Tract
tract_table <- read_civis(x = 'bernie_nmarchio2.geotable_intermed_tract', database = "Bernie 2020")
names(tract_table)
sapply(tract_table, function(x) sum(is.na(x)))
dropcol <- c("county_fip","tract_id_1","gidtr","state","state_name","county","county_name","tract","flag_tract","num_bgs_in_tract_tract","land_area_tract","aian_land_tract")
tract_table2 <- tract_table %>% select(-one_of(dropcol)) 

imputed_data_tract <- mice(tract_table2, m=1, maxit = 1, method = "cart", seed = 500)
complete_data_tract <- mice::complete(imputed_data_tract,1)
sapply(complete_data_tract, function(x) sum(is.na(x)))

write_civis(x=complete_data, tablename='bernie_nmarchio2.geo_county_covariates', database = 'Bernie 2020')
write_civis(x=complete_data_tract, tablename='bernie_nmarchio2.geo_tract_covariates', database = 'Bernie 2020')

# Block check
block_query <- "select
geo.block_group_id
,pdb_bg.* 
from
(select distinct block_group_id from 
((select block_group_id from phoenix_census.geo_identifiers)
union all
(select distinct census_block_group_2010 as block_group_id from phoenix_analytics.person))) geo
left join 
bernie_nmarchio2.census_pdb_blockgroup pdb_bg
on geo.block_group_id = lpad(pdb_bg.gidbg, 12,'000000000000')"

block_table <- read_civis(x = sql(block_query ),database = "Bernie 2020")
sapply(block_table, function(x) sum(is.na(x)))
imputed_data_block <- mice(block_table, m=1, maxit = 1, method = "cart", seed = 500)

names()
complete_data_tract <- mice::complete(imputed_data_tract,1)
sapply(complete_data_tract, function(x) sum(is.na(x)))

names(block_table)

drop_cols <- c('block_group_id','gidbg','state','state_name','county','county_name','tract','block_group','flag','land_area','aian_land')
block_table2 <- block_table %>% select(-one_of(drop_cols)) 


# Join check
qcounts<-'select * from
(select distinct left(block_group_id,11) as tract_g from phoenix_census.geo_identifiers)
full join
(select census_tract_2010 as tract_p, count(*) as ppl from phoenix_analytics.person  where is_deceased = false
  and reg_record_merged = false
  and reg_on_current_file = true 
  and reg_voter_flag = true 
 group by 1)
on  tract_p = tract_g
where tract_g is null or tract_p is null order by ppl desc'

tract_join <- read_civis(x = sql(qcounts), database = "Bernie 2020")

       
# County
#pca <- as.data.frame(prcomp(log(complete_data[, 2:77]),center = TRUE,scale. = TRUE)[["x"]])
pca_county <- prcomp((complete_data[, 2:77]),center = TRUE,scale. = TRUE)
plot(pca_county)

pca_cty_reduced <- as.data.frame(pca_county[["x"]]) %>% select(PC1, PC2, PC3, PC4, PC5) 
complete_data_embed <- merge(complete_data, pca_cty_reduced, by=0, all=TRUE) %>% select(-one_of(c('Row.names'))) 
names(complete_data_embed)
complete_data_embed <- complete_data_embed %>% 
  rename(county_component_pc1 = PC1, 
         county_component_pc2 = PC2, 
         county_component_pc3 = PC3, 
         county_component_pc4 = PC4, 
         county_component_pc5 = PC5)

# Tract
complete_data_tract <- complete_data_tract %>% select(-one_of('tract_id_2')) 
pca_tract <- prcomp((complete_data_tract[,2:75]),center = TRUE,scale. = TRUE)

plot(pca_tract)

pca_tract_reduced <- as.data.frame(pca_tract[["x"]]) %>% select(PC1, PC2, PC3, PC4, PC5) 
complete_data_tract_embed <- merge(complete_data_tract , pca_tract_reduced, by=0, all=TRUE) %>% select(-one_of(c('Row.names'))) 
names(complete_data_tract_embed)
complete_data_tract_embed <- complete_data_tract_embed  %>% 
  rename(tract_component_pc1 = PC1, 
         tract_component_pc2 = PC2, 
         tract_component_pc3 = PC3, 
         tract_component_pc4 = PC4, 
         tract_component_pc5 = PC5)

# Block group
drop_cols <- c('gidbg','state','state_name','county','county_name','tract','block_group','flag','land_area','aian_land')
block_table_pca <- block_table %>% select(-one_of(drop_cols)) 
names(block_table_pca )
block_table_pca <- na.omit(block_table_pca)

pca_block <- prcomp((block_table_pca[,2:115]),center = TRUE,scale. = TRUE)

plot(pca_block)

pca_block_reduced <- as.data.frame(pca_block[["x"]]) %>% select(PC1, PC2, PC3, PC4, PC5) 
complete_data_block_embed <- merge(block_table_pca, pca_block_reduced, by=0, all=TRUE) %>% select(-one_of(c('Row.names'))) 
names(complete_data_block_embed)
complete_data_block_embed <- complete_data_block_embed %>% 
  rename(block_component_pc1 = PC1, 
         block_component_pc2 = PC2, 
         block_component_pc3 = PC3, 
         block_component_pc4 = PC4, 
         block_component_pc5 = PC5)

write_civis(x=complete_data_embed, tablename='bernie_nmarchio2.geo_county_covariates', database = 'Bernie 2020',if_exists = "drop")
write_civis(x=complete_data_tract_embed, tablename='bernie_nmarchio2.geo_tract_covariates', database = 'Bernie 2020',if_exists = "drop")
write_civis(x=complete_data_block_embed, tablename='bernie_nmarchio2.geo_block_covariates', database = 'Bernie 2020',if_exists = "drop")

