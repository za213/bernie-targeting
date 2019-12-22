# Organizing Modeling Workflows

## Platform Links:
* [Project Folder](https://platform.civisanalytics.com/spa/#/projects/129243)

## Modeling Frame:
* Platform table: `bernie_data_commons.rainbow_modeling_frame`
* Script to create [modeling frame](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-modeling-frame.sql)
* [Data dictionary](https://docs.google.com/spreadsheets/d/1O1a4SdNBuPFMRT97__IeD1624OFDFafCSGQAuclDrFU/edit#gid=176972138) 
* Raw data [archived on S3](https://github.com/Bernie-2020/bernie-targeting/blob/master/s3-files/modeling-frame-source-data.R) and [dev scripts for imputating missing data](https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling-frame/dev)
* Dependencies: <small>```phoenix_analytics.person, phoenix_analytics.person_votes, phoenix_scores.all_scores_2020, phoenix_scores.all_scores_2018, phoenix_scores.all_scores_2016, phoenix_consumer.tsmart_consumer, bernie_nmarchio2.geo_county_covariates, bernie_nmarchio2.geo_tract_covariates, bernie_nmarchio2.geo_block_covariates, phoenix_census.acs_current, bernie_data_commons.master_xwalk_dnc, l2.demographics```</small> 

## Modeling Pipelines:

#### Voter Engagement Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/voluntee-dv-recode-v2.sql)
* [Modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/volunteer-modeling-workflow-20191219.ipynb)
* Scores table: `bernie_nmarchio2.actionpop_output_20191220` 
* Validation metrics: `bernie_nmarchio2.actionpop_validation_20191220`

#### Spoke Persuasion Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/spoke-dv-recode.sql)
* [Modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/spoke-modeling-workflow-20191221.ipynb)
* Scores table: `bernie_nmarchio2.spoke_output_20191221` 
* Validation metrics: `bernie_nmarchio2.spoke_validation_20191221`

## Analytics


## Developers
Organizing Analytics Team. Nico Marchio, Data Science Engineer.
