# Organizing Modeling Workflows

## Platform Links:
* [Project Folder](https://platform.civisanalytics.com/spa/#/projects/132631)
  * Workflow to [build modeling and analytics frame](https://platform.civisanalytics.com/spa/#/workflows/10551) then transfer the modeling frame to the Bernie DS cluster. This workflow also re-creates the Base Universe table.
* To request a model, issue a [Jira ticket](https://berniesanders.atlassian.net/jira/software/projects/MOD/boards/12) and tag Nico Marchio and Michael Futch.

## GOTV Universe:
* Visit the [universes](https://github.com/Bernie-2020/bernie-targeting/tree/master/universes) folder for info about the `bernie_data_commons.base_universe` table for cutting GOTV lists.

## Modeling Frame:
* Platform table: `model_inputs.rainbow_modeling_frame` on the Bernie DS Cluster
* [Script](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-modeling-frame.sql) to create modeling frame and [data dictionary](https://docs.google.com/spreadsheets/d/1O1a4SdNBuPFMRT97__IeD1624OFDFafCSGQAuclDrFU/edit#gid=176972138) of features and descriptions
* Table with three default feature lists for modeling `bernie_nmarchio2.feature_list` and here is the [import script](https://platform.civisanalytics.com/spa/#/imports/53801807) to pull in the lists into Platform
* Geographic features: county `bernie_nmarchio2.geo_county_covariates`, tract `bernie_nmarchio2.geo_tract_covariates`, and block group `bernie_nmarchio2.geo_block_covariates` 
* Raw data [archived on S3](https://github.com/Bernie-2020/bernie-targeting/blob/master/s3-files/modeling-frame-source-data.R) and [dev scripts for cleaning and imputing missing data](https://github.com/Bernie-2020/bernie-targeting/tree/master/modeling-frame/dev)
* Data projects: 
  * Engineer features from `phoenix_electionbase.ebase_census_blocks` 
  * Synthesize party switching behavior from `phoenix_voter_file.registration_scd`
  * Test predictive power of `phoenix_consumer.tsmart_consumer`

## Analytics Frame:
* Platform table: `bernie_data_commons.rainbow_analytics_frame`
* [Script](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling-frame/rainbow-analytics-frame.sql) to create analytics frame and [data dictionary](https://docs.google.com/spreadsheets/d/1O1a4SdNBuPFMRT97__IeD1624OFDFafCSGQAuclDrFU/edit#gid=176972138) for list of features

## Modeling Pipelines:

#### Voter Engagement Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/dv-recode/volunteer-dv-recode-v2.sql) and [modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/volunteer-modeling-workflow-20191219.ipynb)
* Scores table: `bernie_nmarchio2.actionpop_output_20191220` 
* Validation metrics: `bernie_nmarchio2.actionpop_validation_20191220`

| Score column | Description | 
| :--- | :--- | 
| `attendee` | Probability to be an attendee of any kind of event |
| `kickoff_party_attendee` | Probability to attend a kickoff party |
| `canvasser_attendee` | Probability to be a canvass volunteer |
| `phonebank_attendee` | Probability to be a phonebank volunteer |
| `rally_barnstorm_attendee` | Probability to attend a rally or barnstorm event |
| `kickoff_party_rally_barnstorm_attendee` | Probability to attend a kickoff party, rally, or barnstorm event |
| `canvasser_phonebank_attendee` | Probability to be a canvass or phonebank volunteer |
| `donor_1plus_times` | Probability to donate at least once |
| `donor_27plus_usd` | Probability to donate more than $27 |
| `bernie_action` | Probability to take any of the above actions |
* Note columns with `_100` are cut into percentiles nationally such that when `where score_100 >= 80` would give the top 20% highest scores.

#### Spoke Persuasion Models:
* [DV recoding script](https://github.com/Bernie-2020/bernie-targeting/blob/master/pipeline-etl/dv-recode/spoke-dv-recode.sql) and [modeling notebook](https://github.com/Bernie-2020/bernie-targeting/blob/master/modeling/spoke-modeling-workflow-20191221.ipynb)
* Scores table: `bernie_nmarchio2.spoke_output_20191221` 
* Validation metrics: `bernie_nmarchio2.spoke_validation_20191221`

| Score column | Description |
| :--- | :--- |
| `spoke_support_1box` | Probability of responding with 1 support |
| `spoke_persuasion_1plus` | Probability of moving 1 or more in favor of Bernie |
| `spoke_persuasion_1minus` | Probability of moving 1 or more against Bernie |
* Note columns with `_100` are cut into percentiles nationally such that when `where score_100 >= 80` would give the top 20% highest scores.

## Analytics
* To enable location based targeting [here is a workflow](https://github.com/Bernie-2020/bernie-targeting/blob/master/analytics/travel-time-targeting_v2.ipynb) that calculates the travel time between each voter within a given commuting radius of a set of points of interest. 

## Developers
Organizing Analytics Team. Nico Marchio, Data Science Engineer.
