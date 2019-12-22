
gender_mobility <- read_civis(x ='bernie_nmarchio2.gender_mobility' ,database = "Bernie 2020")
id <- write_civis_file(gender_mobility, name = "gender_mobility.csv",expires_at = NULL, row.names = FALSE)
# 74838244

geotable_intermed_county <- read_civis(x ='bernie_nmarchio2.geotable_intermed_county' ,database = "Bernie 2020")
id <- write_civis_file(geotable_intermed_county, name = "geotable_intermed_county.csv",expires_at = NULL, row.names = FALSE)
# 74838260

geotable_intermed_tract <- read_civis(x ='bernie_nmarchio2.geotable_intermed_tract' ,database = "Bernie 2020")
id <- write_civis_file(geotable_intermed_tract, name = "geotable_intermed_tract.csv",expires_at = NULL, row.names = FALSE)
# 74838292

neighborhood_effects <- read_civis(x ='bernie_nmarchio2.neighborhood_effects' ,database = "Bernie 2020")
id <- write_civis_file(neighborhood_effects, name = "neighborhood_effects.csv",expires_at = NULL, row.names = FALSE)
# 74838372

tract_covariates <- read_civis(x ='bernie_nmarchio2.tract_covariates' ,database = "Bernie 2020")
id <- write_civis_file(tract_covariates, name = "tract_covariates.csv",expires_at = NULL, row.names = FALSE)
# 74838412

tract_outcomes_simple <- read_civis(x ='bernie_nmarchio2.tract_outcomes_simple' ,database = "Bernie 2020")
id <- write_civis_file(tract_outcomes_simple, name = "tract_outcomes_simple.csv",expires_at = NULL, row.names = FALSE)
# 74838490

census_pdb_blocktract <- read_civis(x ='bernie_nmarchio2.census_pdb_blocktract' ,database = "Bernie 2020")
id <- write_civis_file(census_pdb_blocktract, name = "census_pdb_blocktract.csv",expires_at = NULL, row.names = FALSE)
# 74839908

id <- write_civis_file(census_pdb_blockgroup, name = "census_pdb_blockgroup.csv",expires_at = NULL, row.names = FALSE)
# 74839981

census_pdb_blocktract <- read_civis(x ='bernie_nmarchio2.census_pdb_blocktract' ,database = "Bernie 2020")
id <- write_civis_file(census_pdb_blocktract, name = "census_pdb_blocktract.csv",expires_at = NULL, row.names = FALSE)
# 74840615
