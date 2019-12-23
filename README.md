# Organizing Modeling Workflows

## Platform Links:
* [Project Folder](https://platform.civisanalytics.com/spa/#/projects/129243)

## Modeling Frame:
* Platform table: `bernie_data_commons.rainbow_modeling_frame`
* Script to create [modeling frame](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-modeling-frame.sql) and [data dictionary](https://docs.google.com/spreadsheets/d/1O1a4SdNBuPFMRT97__IeD1624OFDFafCSGQAuclDrFU/edit#gid=176972138) of features
* County, tract, and block group features: <small>```bernie_nmarchio2.geo_county_covariates, bernie_nmarchio2.geo_tract_covariates, bernie_nmarchio2.geo_block_covariates```</small> 
* Raw data [archived on S3](https://github.com/Bernie-2020/bernie-targeting/blob/master/s3-files/modeling-frame-source-data.R) and [dev scripts for cleaning and imputing missing data](https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling-frame/dev)


## Modeling Pipelines:

#### Voter Engagement Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/voluntee-dv-recode-v2.sql) and [modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/volunteer-modeling-workflow-20191219.ipynb)
* Scores table: `bernie_nmarchio2.actionpop_output_20191220` 
* Validation metrics: `bernie_nmarchio2.actionpop_validation_20191220`
 
| Score column | Description | 
| --- | --- | 
| `attendee` | Probability to be an attendee of any kind of event |
| `kickoff_party_attendee` | Probability to attend a kickoff party |
| `canvasser_attendee` | Probability to volunteer to canvass |
| `phonebank_attendee` | Probability to volunteer to phonebank |
| `rally_barnstorm_attendee` | Probability to attend a rally or barnstorm event |
| `kickoff_party_rally_barnstorm_attendee` | Probability to attend a kickoff party, rally, or barnstorm event |
| `canvasser_phonebank_attendee` | Probability to volunteer to canvass or phonebank |
| `donor_1plus_times` | Probability to donate at least once |
| `donor_27plus_usd` | Probability to donate more than $27 |
| `bernie_action` | Probability to take any of the above actions |
<sub> *Note* `_100` represent percentiled scores where the top decile is all values >= 90 </sub> 

#### Spoke Persuasion Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/spoke-dv-recode.sql) and [modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/spoke-modeling-workflow-20191221.ipynb)
* Scores table: `bernie_nmarchio2.spoke_output_20191221` 
* Validation metrics: `bernie_nmarchio2.spoke_validation_20191221`

| Score column | Description |
| --- | --- |
| `spoke_support_1box` | Probability to respond with 1 support |
| `spoke_persuasion_1plus` | Probability of moving 1 or more in favor of Bernie |
| `spoke_persuasion_1minus` | Probability of moving 1 or more against Bernie |
<sub> *Note* `_100` represent percentiled scores where the top decile is all values >= 90 </sub> 

## Analytics


## Developers
Organizing Analytics Team. Nico Marchio, Data Science Engineer.
